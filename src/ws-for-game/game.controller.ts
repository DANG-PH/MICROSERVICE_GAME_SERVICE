import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { WsGateway } from './ws.gateway';
import { NapTienEvent } from 'src/interface/nap.interface';

@Controller()
export class GameConsumer {
  constructor(private readonly wsGateway: WsGateway) {}

  @EventPattern('auth.revoke_all_token')
  async handleRevokeAllToken(
    @Payload() data: { userId: number },
  ) {
    await this.wsGateway.handleRevokeAllToken(data.userId);
  }

  @EventPattern('auth.kick_socket')
  async handleKickSocket(
    @Payload() data: { userId: number },
  ) {
    await this.wsGateway.handleKickSocket(data.userId);
  }

  @EventPattern('user.nap_tien')
  async handleNapTien(
    @Payload() data: { event: NapTienEvent },
  ) {
    await this.wsGateway.handleNapTien(data.event);
  }

  @EventPattern('game.reload_shop')
  async handleReloadShop(
    @Payload() data: { npcId: number },
  ) {
    await this.wsGateway.handleReloadShop(data.npcId);
  }

  @EventPattern('game.notification')
  async handleNotification(
    @Payload() data: { tinNhan: string },
  ) {
    await this.wsGateway.handleNotificationAllUser(data.tinNhan);
  }
}