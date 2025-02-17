/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
import http from 'http';
import net from 'net'
import ws from 'ws';
import { IPC, IpcServer, IpcSocket, IpcSocketId } from 'node-ipc'
import { v4 as uuid } from 'uuid'
import * as yup from 'yup'
import { Assert } from '../../assert'
import { createRootLogger, Logger } from '../../logger'
import { Meter } from '../../metrics/meter'
import { YupUtils } from '../../utils/yup'
import { RpcRequest } from '../request'
import { ApiNamespace, Router } from '../routes'
import { RpcServer } from '../server'
import { IRpcAdapter } from './adapter'
import { ERROR_CODES, ResponseError } from './errors'

export type IpcRequest = {
  mid: number
  type: string
  data: unknown | undefined
}

export type IpcResponse = {
  id: number
  status: number
  data: unknown | undefined
}

export type IpcStream = {
  id: number
  data: unknown | undefined
}

export type IpcError = {
  code: string
  message: string
  stack?: string
}

export const IpcErrorSchema: yup.ObjectSchema<IpcError> = yup
  .object({
    code: yup.string().defined(),
    message: yup.string().defined(),
    stack: yup.string().notRequired(),
  })
  .defined()

export const IpcRequestSchema: yup.ObjectSchema<IpcRequest> = yup
  .object({
    mid: yup.number().required(),
    type: yup.string().required(),
    data: yup.mixed().notRequired(),
  })
  .required()

export const IpcResponseSchema: yup.ObjectSchema<IpcResponse> = yup
  .object({
    id: yup.number().defined(),
    status: yup.number().defined(),
    data: yup.mixed().notRequired(),
  })
  .defined()

export const IpcStreamSchema: yup.ObjectSchema<IpcStream> = yup
  .object({
    id: yup.number().defined(),
    data: yup.mixed().notRequired(),
  })
  .defined()

export type IpcAdapterConnectionInfo =
  | {
    mode: 'ipc'
    socketPath: string
  }
  | {
    mode: 'tcp'
    host: string
    port: number
  }

export class RpcIpcAdapter implements IRpcAdapter {
  router: Router | null = null
  ipc: IPC | null = null
  server: IpcServer | null = null
  httpServer: http.Server | null = null
  wsServer: ws.Server | null = null
  namespaces: ApiNamespace[]
  logger: Logger
  pending = new Map<IpcSocketId, RpcRequest[]>()
  started = false
  connection: IpcAdapterConnectionInfo
  inboundTraffic = new Meter()
  outboundTraffic = new Meter()

  constructor(
    namespaces: ApiNamespace[],
    connection: IpcAdapterConnectionInfo,
    logger: Logger = createRootLogger(),
  ) {
    this.namespaces = namespaces
    this.connection = connection
    this.logger = logger.withTag('ipcadapter')
  }

  async start(): Promise<void> {
    if (this.started) {
      return
    }
    this.started = true

    this.inboundTraffic.start()
    this.outboundTraffic.start()

    const { IPC } = await import('node-ipc')
    const ipc = new IPC()
    ipc.config.silent = true
    ipc.config.rawBuffer = false
    this.ipc = ipc

    return new Promise((resolve, reject) => {
      const onServed = () => {
        const server = ipc.server
        this.server = server

        server.off('error', onError)

        server.on('connect', (socket: IpcSocket) => {
          this.onConnect(socket)
        })

        server.on('socket.disconnected', (socket) => {
          this.onDisconnect(socket, socket.id || null)
        })

        server.on('message', (data: unknown, socket: IpcSocket): void => {
          this.onMessage(socket, data).catch((err) => this.logger.error(err))
        })

        resolve()
      }

      const onError = (error?: unknown) => {
        ipc.server.off('error', onError)
        reject(error)
      }

      if (this.connection.mode === 'ipc') {
        this.logger.debug(`Serving RPC on IPC ${this.connection.socketPath}`)
        ipc.serve(this.connection.socketPath, onServed)
      } else if (this.connection.mode === 'tcp') {
        this.logger.debug(`Serving RPC on TCP ${this.connection.host}:${this.connection.port}`)
        ipc.serveNet(this.connection.host, this.connection.port, onServed)

        // http
        this.logger.debug(`Serving RPC on HTTP ${this.connection.host}:${this.connection.port + 1}`)
        this.httpServer = http.createServer(async (request: http.IncomingMessage, response: http.ServerResponse) => {
          this.logger.trace(`Call HTTP RPC: ${request.method} ${request.url}`)

          const headers = { "Content-Type": "application/json" }

          const requestUrl = new URL('http://localhost' + request.url || '')
          const route = requestUrl.pathname.substring(1)

          // params
          let params: any
          if (requestUrl.search != "") {
            params = {}
            for (const [key, value] of requestUrl.searchParams) {
              params[key] = value
            }
          } else {
            params = undefined
          }
          if (request.method === 'POST' || request.method === 'PUT') {
            // parse body
            const body = [];
            for await (const chunk of request) {
              body.push(chunk)
            }
            params = Object.assign(params || {}, JSON.parse(Buffer.concat(body).toString()))
          }

          const ipcRequest = new Request(
            params,
            (status: number, data?: unknown) => {
              response.writeHead(status, headers)
              response.write(JSON.stringify({ status: status, data: data }))
            },
            (data: unknown) => {
              response.writeHead(200, headers)
              response.write(JSON.stringify({ status: 200, data: data }))
            },
          )
          try {
            await this.router?.route(route, ipcRequest)
          } catch (error: unknown) {
            if (error instanceof ResponseError) {
              response.writeHead(error.status, headers);
              response.write(JSON.stringify({ status: error.status, data: this.renderError(error) }))
            } else {
              throw error
            }
          } finally {
            response.end()
            ipcRequest.close()
          }
        })
        this.wsServer = new ws.Server({ server: this.httpServer })
        this.wsServer.on('connection', async (wsClient: ws, request: http.IncomingMessage) => {
          const requestUrl = new URL('http://localhost' + request.url || '')
          const route = requestUrl.pathname.substring(1)
          if (!route.endsWith("Stream")) {
            wsClient.close()
            return
          }

          // params
          let params: any
          if (requestUrl.search != "") {
            params = {}
            for (const [key, value] of requestUrl.searchParams) {
              params[key] = value
            }
          } else {
            params = undefined
          }

          const ipcRequest = new Request(
            params,
            (status: number, data?: unknown) => void wsClient.send(JSON.stringify({ status, data })),
            (data: unknown) => void wsClient.send(JSON.stringify(data)),
          )
          wsClient.on('close', () => void ipcRequest.close())
          try {
            await this.router?.route(route, ipcRequest)
          } catch (error: unknown) {
            if (error instanceof ResponseError) {
              ipcRequest.end(this.renderError(error), error.status)
              wsClient.close()
            } else {
              wsClient.close()
              throw error
            }
          }
        })

        this.httpServer.on('error', onError)
        this.httpServer.listen(this.connection.port + 1, this.connection.host)
      }

      ipc.server.on('error', onError)
      ipc.server.start()
    })
  }

  async stop(): Promise<void> {
    this.inboundTraffic.stop()
    this.outboundTraffic.stop()

    if (this.started && this.ipc) {
      this.ipc.server.stop()
      this.httpServer?.close()
      this.wsServer?.close()

      for (const socket of this.ipc.server.sockets) {
        Assert.isInstanceOf(socket, net.Socket)
        socket.destroy()
      }

      await this.waitForAllToDisconnect()
    }
  }

  async waitForAllToDisconnect(): Promise<void> {
    if (!this.server) {
      return
    }

    const promises = []

    for (const socket of this.server.sockets) {
      const promise = new Promise<void>((resolve) => {
        const onClose = () => {
          resolve()
          socket.off('close', onClose)
        }
        socket.on('close', onClose)
      })

      promises.push(promise)
    }

    await Promise.all(promises)
  }

  attach(server: RpcServer): void {
    this.router = server.getRouter(this.namespaces)
  }

  onConnect(socket: IpcSocket): void {
    if (!socket.id) {
      socket.id = uuid()
    }
    this.logger.debug(`IPC client connected: ${socket.id}`)
  }

  onDisconnect(socket: IpcSocket, socketId: IpcSocketId | null): void {
    this.logger.debug(`IPC client disconnected: ${socketId ? socketId : 'unknown'}`)

    if (socketId !== null) {
      const pending = this.pending.get(socketId)

      if (pending) {
        for (const request of pending) {
          request.close()
        }
        this.pending.delete(socketId)
      }
    }
  }

  async onMessage(socket: IpcSocket, data: unknown): Promise<void> {
    if (!socket.id) {
      return
    }

    const result = await YupUtils.tryValidate(IpcRequestSchema, data)

    if (result.error) {
      this.handleMalformedRequest(socket, data)
      return
    }

    this.inboundTraffic.add(Buffer.from(JSON.stringify(data)).byteLength)

    const message = result.result
    const router = this.router
    const server = this.server

    Assert.isNotNull(router)
    Assert.isNotNull(server)

    const request = new RpcRequest(
      message.data,
      message.type,
      (status: number, data?: unknown) => {
        this.emitResponse(socket, message.mid, status, data)
      },
      (data: unknown) => {
        this.emitStream(socket, message.mid, data)
      },
    )

    let pending = this.pending.get(socket.id)
    if (!pending) {
      pending = []
      this.pending.set(socket.id, pending)
    }

    pending.push(request)

    try {
      await router.route(message.type, request)
    } catch (error: unknown) {
      if (error instanceof ResponseError) {
        this.emitResponse(socket, message.mid, error.status, this.renderError(error))
      } else {
        throw error
      }
    }
  }

  emitResponse(socket: IpcSocket, messageId: number, status: number, data: unknown): void {
    Assert.isNotNull(this.server)
    this.server.emit(socket, 'message', { id: messageId, status: status, data: data })
    this.outboundTraffic.add(Buffer.from(JSON.stringify(data)).byteLength)
  }

  emitStream(socket: IpcSocket, messageId: number, data: unknown): void {
    Assert.isNotNull(this.server)
    this.server.emit(socket, 'stream', { id: messageId, data: data })
    this.outboundTraffic.add(Buffer.from(JSON.stringify(data)).byteLength)
  }

  renderError(error: Error): IpcError {
    const code = error instanceof ResponseError ? error.code : ERROR_CODES.ERROR

    return {
      code: code,
      message: error.message,
      stack: error.stack,
    }
  }

  handleMalformedRequest(socket: IpcSocket, data: unknown): void {
    Assert.isNotNull(this.server)
    const error = this.renderError(new Error(`Malformed request rejected`))

    if (
      typeof data === 'object' &&
      data !== null &&
      'id' in data &&
      typeof (data as { id: unknown })['id'] === 'number'
    ) {
      const id = (data as { id: unknown })['id'] as number
      this.emitResponse(socket, id, 500, error)
      return
    }

    this.server.emit(socket, 'malformedRequest', error)
    this.outboundTraffic.add(Buffer.from(JSON.stringify(error)).byteLength)
  }
}
