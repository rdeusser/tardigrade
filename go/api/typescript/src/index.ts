import { createClient, type Client, type Interceptor } from "@connectrpc/connect"
import { createConnectTransport } from "@connectrpc/connect-web"
import { HostService } from "./tardigrade/v1/host_pb"

export * from "./tardigrade/v1/host_pb"

// createHostClient returns a HostService client for the host at baseUrl. A token, when set, is sent as a bearer token.
export function createHostClient(baseUrl: string, token?: string): Client<typeof HostService> {
  const interceptors: Interceptor[] = token === undefined ? [] : [next => request => {
    request.header.set("Authorization", `Bearer ${token}`)
    return next(request)
  }]
  return createClient(HostService, createConnectTransport({ baseUrl, interceptors }))
}
