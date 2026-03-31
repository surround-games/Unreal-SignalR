/*
 * MIT License
 *
 * Copyright (c) 2020-2022 Frozen Storm Interactive, Yoann Potinet
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include "Connection.h"
#include "HttpModule.h"
#include "Interfaces/IHttpRequest.h"
#include "WebSocketsModule.h"
#include "SignalRModule.h"
#include "Interfaces/IHttpResponse.h"
#include "Interfaces/IHttpRequest.h"
#include "Dom/JsonObject.h"
#include "Serialization/JsonReader.h"
#include "Serialization/JsonSerializer.h"

namespace
{
    FString BuildNegotiationErrorMessage(const FString& Message, const FHttpResponsePtr& Response)
    {
        FString ErrorMessage = Message;

        if (Response.IsValid())
        {
            const int32 StatusCode = Response->GetResponseCode();
            if (StatusCode > 0)
            {
                ErrorMessage = FString::Printf(TEXT("%s (HTTP %d)"), *ErrorMessage, StatusCode);
            }

            FString ResponseBody = Response->GetContentAsString().TrimStartAndEnd();
            if (!ResponseBody.IsEmpty())
            {
                constexpr int32 MaxBodyLength = 256;
                if (ResponseBody.Len() > MaxBodyLength)
                {
                    ResponseBody = ResponseBody.Left(MaxBodyLength) + TEXT("...");
                }

                ErrorMessage += FString::Printf(TEXT(" Body: %s"), *ResponseBody);
            }
        }

        return ErrorMessage;
    }
}

FConnection::FConnection(const FString& InHost, const TMap<FString, FString>& InHeaders):
    Host(InHost),
    Headers(InHeaders)
{
}

void FConnection::Connect()
{
    Negotiate();
}

bool FConnection::IsConnected()
{
    return Connection.IsValid() && Connection->IsConnected();
}

void FConnection::Send(const FString& Data)
{
    if (Connection.IsValid())
    {
        Connection->Send(Data);
    }
    else
    {
        UE_LOG(LogSignalR, Error, TEXT("Cannot send data to non connected websocket."));
    }
}

void FConnection::Close(int32 Code, const FString& Reason)
{
    if(Connection.IsValid())
    {
        Connection->Close(Code, Reason);
    }
    else
    {
        UE_LOG(LogSignalR, Error, TEXT("Cannot close non connected websocket."));
    }
}

FConnection::FConnectionFailedEvent& FConnection::OnConnectionFailed()
{
    return OnConnectionFailedEvent;
}

IWebSocket::FWebSocketConnectedEvent& FConnection::OnConnected()
{
    return OnConnectedEvent;
}

IWebSocket::FWebSocketConnectionErrorEvent& FConnection::OnConnectionError()
{
    return OnConnectionErrorEvent;
}

IWebSocket::FWebSocketClosedEvent& FConnection::OnClosed()
{
    return OnClosedEvent;
}

IWebSocket::FWebSocketMessageEvent& FConnection::OnMessage()
{
    return OnMessageEvent;
}

void FConnection::Negotiate()
{
    TSharedRef<IHttpRequest, ESPMode::ThreadSafe> HttpRequest = FHttpModule::Get().CreateRequest();

    HttpRequest->SetVerb(TEXT("POST"));
    HttpRequest->OnProcessRequestComplete().BindSP(AsShared(), &FConnection::OnNegotiateResponse);
    for (TTuple<FString, FString> Header : Headers)
    {
        HttpRequest->SetHeader(Header.Key, Header.Value);
    }
    HttpRequest->SetURL(Host + TEXT("/negotiate?negotiateVersion=1"));
    HttpRequest->ProcessRequest();
}

void FConnection::OnNegotiateResponse(FHttpRequestPtr InRequest, FHttpResponsePtr InResponse, bool bConnectedSuccessfully)
{
    if (!bConnectedSuccessfully)
    {
        const FString ErrorMessage = BuildNegotiationErrorMessage(TEXT("Could not connect to host during negotiate."), InResponse);
        UE_LOG(LogSignalR, Error, TEXT("%s"), *ErrorMessage);
        OnConnectionErrorEvent.Broadcast(ErrorMessage);
        return;
    }

    if (!InResponse.IsValid())
    {
        const FString ErrorMessage = TEXT("Negotiate failed without an HTTP response.");
        UE_LOG(LogSignalR, Error, TEXT("%s"), *ErrorMessage);
        OnConnectionErrorEvent.Broadcast(ErrorMessage);
        return;
    }

    if(InResponse->GetResponseCode() != 200)
    {
        const FString ErrorMessage = BuildNegotiationErrorMessage(TEXT("Negotiate failed."), InResponse);
        UE_LOG(LogSignalR, Error, TEXT("%s"), *ErrorMessage);
        OnConnectionErrorEvent.Broadcast(ErrorMessage);
        return;
    }

    TSharedPtr<FJsonObject> JsonObject = MakeShareable(new FJsonObject());
    TSharedRef<TJsonReader<>> JsonReader = TJsonReaderFactory<>::Create(InResponse->GetContentAsString());

    if (FJsonSerializer::Deserialize(JsonReader, JsonObject) && JsonObject.IsValid())
    {
        if(JsonObject->HasField(TEXT("error")))
        {
            const FString ErrorMessage = BuildNegotiationErrorMessage(
                FString::Printf(TEXT("Negotiate returned an error: %s"), *JsonObject->GetStringField(TEXT("error"))),
                InResponse);
            UE_LOG(LogSignalR, Error, TEXT("%s"), *ErrorMessage);
            OnConnectionErrorEvent.Broadcast(ErrorMessage);
            return;
        }
        else
        {
            if (JsonObject->HasField(TEXT("ProtocolVersion")))
            {
                const FString ErrorMessage = TEXT("Detected an ASP.NET SignalR server. This client only supports ASP.NET Core SignalR.");
                UE_LOG(LogSignalR, Error, TEXT("%s"), *ErrorMessage);
                OnConnectionErrorEvent.Broadcast(ErrorMessage);
                return;
            }

            if (JsonObject->HasTypedField<EJson::String>(TEXT("url")))
            {
                FString RedirectionUrl = JsonObject->GetStringField(TEXT("url"));
                const FString ErrorMessage = FString::Printf(TEXT("SignalR negotiate redirection is not supported. Redirect url: %s"), *RedirectionUrl);
                UE_LOG(LogSignalR, Error, TEXT("%s"), *ErrorMessage);
                OnConnectionErrorEvent.Broadcast(ErrorMessage);
                return;
            }

            if (JsonObject->HasTypedField<EJson::Array>(TEXT("availableTransports")))
            {
                // check if support WebSockets with Text format
                bool bIsCompatible = false;
                for (TSharedPtr<FJsonValue> TransportData : JsonObject->GetArrayField(TEXT("availableTransports")))
                {
                    if(TransportData.IsValid() && TransportData->Type == EJson::Object)
                    {
                        TSharedPtr<FJsonObject> TransportObj = TransportData->AsObject();
                        if(TransportObj->HasTypedField<EJson::String>(TEXT("transport")) && TransportObj->GetStringField(TEXT("transport")) == TEXT("WebSockets") && TransportObj->HasTypedField<EJson::Array>(TEXT("transferFormats")))
                        {
                            for (TSharedPtr<FJsonValue> TransportFormatData : TransportObj->GetArrayField(TEXT("transferFormats")))
                            {
                                if (TransportFormatData.IsValid() && TransportFormatData->Type == EJson::String && TransportFormatData->AsString() == TEXT("Text"))
                                {
                                    bIsCompatible = true;
                                }
                            }
                        }
                    }
                }

                if(!bIsCompatible)
                {
                    const FString ErrorMessage = TEXT("The server does not support WebSockets with Text transfer format.");
                    UE_LOG(LogSignalR, Error, TEXT("%s"), *ErrorMessage);
                    OnConnectionErrorEvent.Broadcast(ErrorMessage);
                    return;
                }
            }

            if (JsonObject->HasTypedField<EJson::String>(TEXT("connectionId")))
            {
                ConnectionId = JsonObject->GetStringField(TEXT("connectionId"));
            }

            if (JsonObject->HasTypedField<EJson::String>(TEXT("connectionToken")))
            {
                ConnectionId = JsonObject->GetStringField(TEXT("connectionToken"));
            }

            StartWebSocket();
        }
    }
    else
    {
        const FString ErrorMessage = BuildNegotiationErrorMessage(TEXT("Cannot parse negotiate response."), InResponse);
        UE_LOG(LogSignalR, Error, TEXT("%s"), *ErrorMessage);
        OnConnectionErrorEvent.Broadcast(ErrorMessage);
    }
}

void FConnection::StartWebSocket()
{
    const FString COnver = ConvertToWebsocketUrl(Host);
    Connection = FWebSocketsModule::Get().CreateWebSocket(COnver, FString(), Headers);

    if(Connection.IsValid())
    {
        Connection->OnConnected().AddLambda([Self = TWeakPtr<FConnection>(AsShared())]()
        {
            if (TSharedPtr<FConnection> SharedSelf = Self.Pin())
            {
                SharedSelf->OnConnectedEvent.Broadcast();
            }
        });
        Connection->OnConnectionError().AddLambda([Self = TWeakPtr<FConnection>(AsShared())](const FString& ErrString)
        {
            UE_LOG(LogSignalR, Warning, TEXT("Websocket err: %s"), *ErrString);

            if (TSharedPtr<FConnection> SharedSelf = Self.Pin())
            {
                SharedSelf->OnConnectionErrorEvent.Broadcast(ErrString);
            }
        });
        Connection->OnClosed().AddLambda([Self = TWeakPtr<FConnection>(AsShared())](int32 StatusCode, const FString& Reason, bool bWasClean)
        {
            if (TSharedPtr<FConnection> SharedSelf = Self.Pin())
            {
                SharedSelf->OnClosedEvent.Broadcast(StatusCode, Reason, bWasClean);
            }
        });
        Connection->OnMessage().AddLambda([Self = TWeakPtr<FConnection>(AsShared())](const FString& MessageString)
        {
            if (TSharedPtr<FConnection> SharedSelf = Self.Pin())
            {
                SharedSelf->OnMessageEvent.Broadcast(MessageString);
            }
        });

        Connection->Connect();
    }
    else
    {
        const FString ErrorMessage = TEXT("Cannot start websocket.");
        UE_LOG(LogSignalR, Error, TEXT("%s"), *ErrorMessage);
        OnConnectionErrorEvent.Broadcast(ErrorMessage);
    }
}

FString FConnection::ConvertToWebsocketUrl(const FString& Url)
{
    const FString TrimmedUrl = Url.TrimStartAndEnd();

    if (TrimmedUrl.StartsWith(TEXT("https://")))
    {
        return TEXT("wss") + TrimmedUrl.RightChop(5);
    }
    else if (TrimmedUrl.StartsWith(TEXT("http://")))
    {
        return TEXT("ws") + TrimmedUrl.RightChop(4);
    }
    else
    {
        return Url;
    }
}
