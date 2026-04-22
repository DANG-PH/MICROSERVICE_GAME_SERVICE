import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { RedisIoAdapter } from './redis-io.adapter';
import { MicroserviceOptions, Transport } from '@nestjs/microservices'; 

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  const redisIoAdapter = new RedisIoAdapter(app);
  await redisIoAdapter.connectToRedis();
  app.useWebSocketAdapter(redisIoAdapter);

  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.RMQ,
    options: {
      urls: [String(process.env.RABBIT_URL)],
      queue: process.env.RABBIT_GAME_QUEUE,
      queueOptions: { durable: true },
    },
  });

  await app.startAllMicroservices(); 
  await app.listen(Number(process.env.PORT), '0.0.0.0');
}
bootstrap();