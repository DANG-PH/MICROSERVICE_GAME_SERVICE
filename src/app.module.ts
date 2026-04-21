import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { UserModule } from './user/user.module';
import { WsModule } from './ws-for-game/ws.module';

@Module({
  imports: [
    UserModule,
    WsModule,
  ],
  controllers: [AppController],
  providers: [],
})

export class AppModule {}
