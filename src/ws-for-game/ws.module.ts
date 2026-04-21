import { forwardRef, Module } from "@nestjs/common";
import { WsJwtGuard } from "src/guard/ws.guard";
import { JwtService } from "@nestjs/jwt";
import { UserModule } from "../user/user.module";
import { WsGateway } from "./ws.gateway";
import { ClientsModule, Transport } from "@nestjs/microservices";

@Module({
    imports: [
        ClientsModule.register([
            {
                name: String(process.env.RABBIT_SERVICE),
                transport: Transport.RMQ,
                options: {
                urls: [String(process.env.RABBIT_URL)],
                queue: process.env.RABBIT_QUEUE,
                queueOptions: { durable: true },
                },
            },
        ]),
        UserModule,
    ],
    controllers: [],
    providers: [WsGateway, WsJwtGuard, JwtService],
    exports: [WsGateway]
})
export class WsModule{};