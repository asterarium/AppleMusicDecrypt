import asyncio
import json
from typing import Awaitable, Callable, Type

from async_lru import alru_cache
from creart import AbstractCreator, CreateTargetInfo, exists_module, it
from grpc import ssl_channel_credentials, StatusCode
from grpc.aio import AioRpcError, insecure_channel, Channel, secure_channel
from grpc.experimental import ChannelOptions
from tenacity import retry, retry_if_exception, wait_random_exponential, stop_after_attempt, before_sleep_log

from src.grpc.manager_pb2 import *
from src.grpc.manager_pb2_grpc import WrapperManagerServiceStub, google_dot_protobuf_dot_empty__pb2
from src.logger import GlobalLogger
from src.config import Config
from src.runtime import configure_grpc_proxy_environment
from src.utils import safely_create_task


class WrapperManagerException(Exception):
    def __init__(self, msg: str):
        self.msg = msg


RETRYABLE_RPC_STATUS_CODES = {
    StatusCode.UNAVAILABLE,
    StatusCode.INTERNAL,
    StatusCode.RESOURCE_EXHAUSTED,
    StatusCode.DEADLINE_EXCEEDED,
}
NON_RETRYABLE_WRAPPER_MESSAGES = ("no available instance", "no such account")


def _is_retryable_rpc_exception(exc: Exception) -> bool:
    if isinstance(exc, WrapperManagerException):
        message = exc.msg.lower()
        return not any(marker in message for marker in NON_RETRYABLE_WRAPPER_MESSAGES)
    if isinstance(exc, AioRpcError):
        details = (exc.details() or "").lower()
        if exc.code() in RETRYABLE_RPC_STATUS_CODES:
            return True
        return "status: 429" in details or "too many requests" in details
    return False


class WrapperManager:
    _channel: Channel
    _stub: WrapperManagerServiceStub
    _decrypt_queue: asyncio.Queue[DecryptRequest]
    _login_lock: asyncio.Lock
    _target_url: str
    _secure: bool
    _explicit_proxy: str | None

    def __init__(self):
        self._login_lock = asyncio.Lock()
        self._decrypt_queue = asyncio.Queue()
        self._target_url = ""
        self._secure = False
        self._explicit_proxy = None

    async def init(self, url: str, secure: bool, proxy: str = ""):
        try:
            self._explicit_proxy = configure_grpc_proxy_environment(url, proxy)
        except ValueError as exc:
            raise WrapperManagerException(str(exc)) from exc

        self._target_url = url
        self._secure = secure
        self._build_channel()
        return self

    def _build_channel(self):
        service_config_json = json.dumps(
            {
                "methodConfig": [
                    {
                        "name": [{}],
                        "retryPolicy": {
                            "maxAttempts": 5,
                            "initialBackoff": "0.1s",
                            "maxBackoff": "1s",
                            "backoffMultiplier": 2,
                            "retryableStatusCodes": ["UNAVAILABLE", "INTERNAL"],
                        },
                    }
                ]
            }
        )
        options = ((ChannelOptions.SingleThreadedUnaryStream, 1), ("grpc.service_config", service_config_json))
        if self._secure:
            self._channel = secure_channel(self._target_url, credentials=ssl_channel_credentials(), options=options)
        else:
            self._channel = insecure_channel(self._target_url, options=options)
        self._stub = WrapperManagerServiceStub(self._channel)

    async def _reconnect_decrypt_channel(self, retry_delay: float):
        await asyncio.sleep(retry_delay)
        try:
            await self._channel.close()
        except Exception:
            pass
        self._build_channel()

    @alru_cache
    @retry(retry=retry_if_exception(_is_retryable_rpc_exception),
           wait=wait_random_exponential(multiplier=1, max=it(Config).download.maxWaitTime),
           stop=stop_after_attempt(it(Config).download.retryTime),
           before_sleep=before_sleep_log(it(GlobalLogger).logger, "WARNING"))
    async def status(self) -> StatusData:
        resp: StatusReply = await self._stub.Status(google_dot_protobuf_dot_empty__pb2.Empty)
        if resp.header.code != 0:
            raise WrapperManagerException(resp.header.msg)
        return resp.data

    async def login(self, username: str, password: str, on_2fa: Callable[[str, str], Awaitable[str]]):
        await self._login_lock.acquire()

        login_queue = asyncio.Queue()

        async def request_stream():
            while True:
                item = await login_queue.get()
                if item is None:
                    break
                yield item

        stream = self._stub.Login(request_stream())

        await login_queue.put(LoginRequest(data=LoginData(username=username, password=password)))

        async for reply in stream:
            reply: LoginReply
            match reply.header.code:
                case -1:
                    self._login_lock.release()
                    await login_queue.put(None)
                    raise WrapperManagerException(reply.header.msg)
                case 0:
                    self._login_lock.release()
                    await login_queue.put(None)
                    return
                case 2:
                    two_step_code = await on_2fa(username, password)
                    await login_queue.put(LoginRequest(data=LoginData(
                        username=username,
                        password=password,
                        two_step_code=two_step_code)))

    async def decrypt(self, adam_id: str, key: str, sample: bytes, sample_index: int):
        await self._decrypt_queue.put(
            DecryptRequest(data=DecryptData(adam_id=adam_id, key=key, sample_index=sample_index,
                                            sample=sample)))

    async def _decrypt_request_generator(self):
        while True:
            yield await self._decrypt_queue.get()

    async def decrypt_init(self,
                           on_success: Callable[[str, str, bytes, int], Awaitable[None]],
                           on_failure: Callable[[str, str, bytes, int, str], Awaitable[None]],
                           on_stream_error: Callable[[Exception], Awaitable[None]]):
        retry_delay = 1.0
        while True:
            keepalive_task = asyncio.create_task(self._decrypt_keepalive())
            stream = self._stub.Decrypt(self._decrypt_request_generator())
            try:
                async for reply in stream:
                    reply: DecryptReply
                    retry_delay = 1.0
                    if reply.data.adam_id == "KEEPALIVE":
                        continue
                    match reply.header.code:
                        case -1:
                            safely_create_task(
                                on_failure(
                                    reply.data.adam_id,
                                    reply.data.key,
                                    reply.data.sample,
                                    reply.data.sample_index,
                                    reply.header.msg,
                                )
                            )
                        case 0:
                            safely_create_task(
                                on_success(reply.data.adam_id, reply.data.key, reply.data.sample, reply.data.sample_index))
            except asyncio.CancelledError:
                raise
            except AioRpcError as exc:
                self._log_rpc_error("Decrypt stream terminated", exc)
                self._clear_decrypt_queue()
                await on_stream_error(exc)
                await self._reconnect_decrypt_channel(retry_delay)
                retry_delay = min(retry_delay * 2, it(Config).download.maxWaitTime)
            else:
                stream_closed = WrapperManagerException("Decrypt stream closed unexpectedly")
                it(GlobalLogger).logger.error(stream_closed.msg)
                self._clear_decrypt_queue()
                await on_stream_error(stream_closed)
                await self._reconnect_decrypt_channel(retry_delay)
                retry_delay = min(retry_delay * 2, it(Config).download.maxWaitTime)
            finally:
                keepalive_task.cancel()
                await asyncio.gather(keepalive_task, return_exceptions=True)

    async def _decrypt_keepalive(self):
        while True:
            await self._decrypt_queue.put(DecryptRequest(data=DecryptData(adam_id="KEEPALIVE")))
            await asyncio.sleep(15)

    def _clear_decrypt_queue(self):
        while not self._decrypt_queue.empty():
            try:
                self._decrypt_queue.get_nowait()
            except asyncio.QueueEmpty:
                break

    @retry(retry=retry_if_exception(_is_retryable_rpc_exception),
           wait=wait_random_exponential(multiplier=1, max=it(Config).download.maxWaitTime),
           stop=stop_after_attempt(it(Config).download.retryTime), before_sleep=before_sleep_log(it(GlobalLogger).logger, "WARNING"))
    async def m3u8(self, adam_id: str) -> str:
        resp: M3U8Reply = await self._stub.M3U8(M3U8Request(data=M3U8DataRequest(adam_id=adam_id)))
        if resp.header.code != 0:
            raise WrapperManagerException(resp.header.msg)
        return resp.data.m3u8

    @retry(retry=retry_if_exception(_is_retryable_rpc_exception),
           wait=wait_random_exponential(multiplier=1, max=it(Config).download.maxWaitTime),
           stop=stop_after_attempt(it(Config).download.retryTime), before_sleep=before_sleep_log(it(GlobalLogger).logger, "WARNING"))
    async def logout(self, username: str):
        resp: LogoutReply = await self._stub.Logout(LogoutRequest(data=LogoutData(username=username)))
        if resp.header.code != 0:
            raise WrapperManagerException(resp.header.msg)
        return

    @retry(retry=retry_if_exception(_is_retryable_rpc_exception),
           wait=wait_random_exponential(multiplier=1, max=it(Config).download.maxWaitTime),
           stop=stop_after_attempt(it(Config).download.retryTime), before_sleep=before_sleep_log(it(GlobalLogger).logger, "WARNING"))
    async def lyrics(self, adam_id: str, language: str, region: str) -> str:
        resp: LyricsReply = await self._stub.Lyrics(LyricsRequest(
            data=LyricsDataRequest(adam_id=adam_id, language=language, region=region)))
        if resp.header.code != 0:
            raise WrapperManagerException(resp.header.msg)
        return resp.data.lyrics

    @retry(retry=retry_if_exception(_is_retryable_rpc_exception),
           wait=wait_random_exponential(multiplier=1, max=it(Config).download.maxWaitTime),
           stop=stop_after_attempt(it(Config).download.retryTime), before_sleep=before_sleep_log(it(GlobalLogger).logger, "WARNING"))
    async def webPlayback(self, adam_id: str) -> str:
        resp: WebPlaybackReply = await self._stub.WebPlayback(WebPlaybackRequest(
            data=WebPlaybackDataRequest(adam_id=adam_id)
        ))
        if resp.header.code != 0:
            raise WrapperManagerException(resp.header.msg)
        return resp.data.m3u8

    @retry(retry=retry_if_exception(_is_retryable_rpc_exception),
           wait=wait_random_exponential(multiplier=1, max=it(Config).download.maxWaitTime),
           stop=stop_after_attempt(it(Config).download.retryTime), before_sleep=before_sleep_log(it(GlobalLogger).logger, "WARNING"))
    async def license(self, adam_id: str, challenge: str, kid: str) -> str:
        resp: LicenseReply = await self._stub.License(LicenseRequest(
            data=LicenseDataRequest(adam_id=adam_id, challenge=challenge, uri=kid)
        ))
        if resp.header.code != 0:
            raise WrapperManagerException(resp.header.msg)
        return resp.data.license

    def _log_rpc_error(self, action: str, exc: AioRpcError) -> None:
        proxy_state = self._explicit_proxy if self._explicit_proxy else "system/default"
        it(GlobalLogger).logger.error(
            f"{action}: target={self._target_url}, secure={self._secure}, grpc_proxy={proxy_state}, "
            f"code={exc.code().name}, details={exc.details()}"
        )
        details = exc.details() or ""
        debug_error = exc.debug_error_string() or ""
        if "RST_STREAM with error code 2" in details or 'Failed "execute_batch"' in debug_error:
            it(GlobalLogger).logger.error(
                "The wrapper-manager gRPC stream looks like it was interrupted by a proxy or HTTP/2 CONNECT incompatibility."
            )
        if "status: 429" in details or "received http2 header with status: 429" in details:
            it(GlobalLogger).logger.error(
                "The wrapper-manager or an upstream proxy returned HTTP 429. This is treated as a retryable transport error."
            )


class WMCreator(AbstractCreator):
    targets = (
        CreateTargetInfo("src.grpc.manager", "WrapperManager"),
    )

    @staticmethod
    def available() -> bool:
        return exists_module("src.grpc.manager")

    @staticmethod
    def create(create_type: Type[WrapperManager]) -> WrapperManager:
        return create_type()
