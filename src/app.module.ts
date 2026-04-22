import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { UserModule } from './user/user.module';
import { WsModule } from './ws-for-game/ws.module';
import { RedisModule } from './redis/redis.module';

@Module({
  imports: [
    UserModule,
    WsModule,
    RedisModule,
  ],
  controllers: [AppController],
  providers: [],
})

export class AppModule {}
