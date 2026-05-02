import {
  WebSocketGateway,
  WebSocketServer,
  SubscribeMessage,
  ConnectedSocket,
  MessageBody,
  OnGatewayInit,
} from '@nestjs/websockets';
import { Server, Socket } from 'socket.io';
import { WsJwtGuard } from 'src/guard/ws.guard';
import { UseGuards } from '@nestjs/common';
import Redis from 'ioredis';
import { Inject } from '@nestjs/common';
import { CACHE_MANAGER, Cache } from '@nestjs/cache-manager';
import { JwtService } from '@nestjs/jwt';
import { UserService } from '../user/user.service';
import { Double } from 'mongodb';
import { Item } from 'proto/item.pb';
import { v4 as uuidv4 } from 'uuid';
import { ClientProxy } from '@nestjs/microservices';
import { LoaiNapTien } from 'src/enums/nap.enum';
import type { NapTienEvent } from 'src/interface/nap.interface';
import { ALLOWED_COSMETIC_FIELDS, CosmeticField } from 'src/enums/cosmetic.enum';
import { Cron, CronExpression } from '@nestjs/schedule';
import Redlock, { ResourceLockedError, ExecutionError, Lock as RLock } from 'redlock';

@UseGuards(WsJwtGuard)
@WebSocketGateway({
  namespace: '/ws-game',
  pingTimeout: 10000,   // chờ 10s không có pong → disconnect
  pingInterval: 5000,   // ping mỗi 5s
})
export class WsGateway {
  @WebSocketServer()
  server: Server;
  private redlock: Redlock;

  constructor(
    @Inject(CACHE_MANAGER) private cacheManager: Cache,
    private readonly jwtService: JwtService,
    private readonly userService: UserService,
    @Inject(String(process.env.RABBIT_SERVICE)) private readonly queueClient: ClientProxy,
    @Inject('REDIS_CLIENT') private readonly redis: Redis,
  ) {
    this.redlock = new Redlock([this.redis], { retryCount: 0 }); // 1 node redis
    // retryCount: 0 nghĩa là thử acquire lock đúng 1 lần, nếu thất bại thì throw ngay, không retry.
  }

  async handleConnection(client: Socket) {
    try {
      const token = 
              client.handshake.auth?.token ||
              client.handshake.query?.token ||
              client.handshake.headers?.authorization?.split(' ')[1];
      if (!token) {
        client.disconnect(); 
        return;
      }
      
      const payload = await this.jwtService.verifyAsync(token, { secret: process.env.JWT_SECRET });

      // Lấy gameSessionId từ handshake thay vì từ JWT
      const gameSessionId = client.handshake.auth?.gameSessionId;
      if (!gameSessionId) {
        client.disconnect();
        return;
      }

      // Check gameSessionId có match với session đang active của user không
      const currentGameSessionId = await this.redis.get(`user:${payload.userId}:gameSession`);
      if (currentGameSessionId !== gameSessionId) {
        client.disconnect();
        return;
      }

      client.data.user = { ...payload, gameSessionId };

      const userId = payload.userId;
      const state = await this.userService.handleGetPosition({ userId });

      await this.redis.hset(`GAME:PLAYER:${userId}`, {
        x: state.x,
        y: state.y,
        map: state.map,
        trangthai: 'DUNG_YEN',
        dir: 1,
        dau: "nhanvat/traidat/avatar/Goku_base/daudung.png",
        than: "nhanvat/traidat/do/set_base/thandung.png",
        chan: "nhanvat/traidat/do/set_base/chandung.png",
        timeChoHienBay: 0,
        lechDauX: -0.3,
        lechDauY: 15.5,
        lechThanX: 0,
        lechThanY: 0,
        lechChanX: 0,
        lechChanY: 0,
        dangMangVanBay: false,
        tenVanBay: "base",
        rong: 50,
        cao: 50,
        gameName: state.gameName,
        avatar: "nhanvat/traidat/avatar/Goku_base/daudung.png",
      });

      await this.redis.sadd(`GAME:MAP:${state.map}`, userId);

      client.join(`MAP:${state.map}`);
      client.data.map = state.map;

      const players = await this.getPlayersInMap(state.map);
      client.emit('mapSnapshot', players);
      await this.syncSkillsToClient(client, state.map);
      const rongThanRaw = await this.redis.get(this.RONG_THAN_KEY);
      if (rongThanRaw) {
        const { userId: ownerUserId, map: rongMap, ngocRongUoc, x, y, gameName } = JSON.parse(rongThanRaw);
        if (rongMap === state.map) {
          client.emit('uocRongThan', { 
            mapToi: true, 
            nguoiUoc: ownerUserId, 
            gameNameNguoiUoc: gameName,
            map: rongMap, 
            x: x,
            y: y,
            ngocRongUoc: ngocRongUoc,
          });
        }
      } else {
        // Key không còn -> reset state client phòng trường hợp client còn state cũ
        client.emit('uocRongThan', {
          mapToi: false,
          nguoiUoc: null,
          gameNameNguoiUoc: null,
          map: state.map,
          x: 0,
          y: 0,
          ngocRongUoc: "",
        });
      }

      client.to(`MAP:${state.map}`).emit('playerSpawn', {
        userId,
        x: state.x,
        y: state.y,
        trangthai: 'DUNG_YEN',
        dir: 1,
        dau: "",
        than: "",
        chan: "", // Gửi rỗng để tạm thời k render ra ảnh, 
                  // trước để ảnh base render ra khá xấu và k đúng ảnh hiện tại của user
                  // Vậy tại sao k comment cả emit này mà vẫn gửi nhưng gửi rỗng?
                  // Vì gửi để người chơi khác còn put vào ArrayList playerState của họ, và lúc đó người chơi gửi sync lúc vừa vào game (do lastsentX đang -9999 nên mới vào game sẽ gửi sync 1 lần, lúc này đúng quần áo, trạng thái,...)
                  // Còn nếu k gửi thì khi người chơi gửi sync (vẫn gửi do lastsent = -9999) nhưng người chơi khác sẽ k chấp nhận vì nếu k emit ở đây thì sẽ chưa được push vào list playerState của người chơi khác dẫn đến bị reject
        timeChoHienBay: 0,
        lechDauX: -0.3,
        lechDauY: 15.5,
        lechThanX: 0,
        lechThanY: 0,
        lechChanX: 0,
        lechChanY: 0,
        dangMangVanBay: false,
        tenVanBay: "base",
        rong: 50,
        cao: 50,
        gameName: state.gameName,
        avatar: "nhanvat/traidat/avatar/Goku_base/daudung.png",
        // deoLungDung: null, client tự biết null, k cần gửi
      });

      client.join(`Game:${payload.userId}`);
      client.join(`NotificationGame`);

      if (payload.role === 'ADMIN') {
        client.to(`NotificationGame`).emit('notification', { tinNhan: `Đại đế ${state.gameName} đã online tại ${state.map}` });
      }
    } catch (e) {
      client.disconnect(); 
    }
  }

  async handleDisconnect(client: Socket) {
    console.log('handleDisconnect called, userId:', client.data.user?.userId);
    const userId = client.data.user?.userId;
    const map = client.data.map;
    if (!userId) return;

    const state = await this.redis.hgetall(`GAME:PLAYER:${userId}`);
    if (!state) return;

    await this.userService.handleSavePosition({
      userId,
      x: Number(state.x),
      y: Number(state.y),
      map: state.map,
    });

    await this.redis.del(`GAME:PLAYER:${userId}`);

    if (map) {
      await this.redis.srem(`GAME:MAP:${map}`, userId);
      client.to(`MAP:${map}`).emit('playerDespawn', { userId });
    }
  }

  @SubscribeMessage('setMap')
  async handleSetMap(
    @ConnectedSocket() client: Socket,
    @MessageBody() body: { oldMap:string, map: string, x: number, y: number, dir: number, trangthai: string, dau: string, than: string, chan: string, timeChoHienBay: Double, lechDauX: Double, lechDauY: Double, lechThanX: Double, lechThanY: Double, lechChanX: Double, lechChanY: Double, dangMangVanBay: string, tenVanBay: string, rong: Double, cao: Double, avatar: string },
  ) {
    const userId = client.data.user.userId;

    const state = await this.redis.hgetall(`GAME:PLAYER:${userId}`);
    await this.userService.handleSavePosition({
      userId,
      x: Number(state.x),
      y: Number(state.y),
      map: body.oldMap,
    });

    if (body.oldMap) {
      await this.redis.srem(`GAME:MAP:${body.oldMap}`, userId);
      client.leave(`MAP:${body.oldMap}`);
      client.to(`MAP:${body.oldMap}`).emit('playerDespawn', { userId });
    }

    await this.redis.hset(`GAME:PLAYER:${userId}`, {
      map: body.map,
      x: body.x,
      y: body.y,
      trangthai: body.trangthai,
      dir: body.dir,
      dau: body.dau,
      than: body.than,
      chan: body.chan,
      timeChoHienBay: body.timeChoHienBay,
      lechDauX: body.lechDauX,
      lechDauY: body.lechDauY,
      lechThanX: body.lechThanX,
      lechThanY: body.lechThanY,
      lechChanX: body.lechChanX,
      lechChanY: body.lechChanY,
      dangMangVanBay: body.dangMangVanBay,
      tenVanBay: body.tenVanBay,
      rong: body.rong,
      cao: body.cao,
      gameName: state.gameName,
      avatar: body.avatar,
      deoLungDung: state.deoLungDung ?? null,
      huyHieuDung: state.huyHieuDung ?? null,
      auraDung: state.auraDung ?? null,
    });

    await this.redis.sadd(`GAME:MAP:${body.map}`, userId);

    client.join(`MAP:${body.map}`);
    client.data.map = body.map;

    const players = await this.getPlayersInMap(body.map);
    client.emit('mapSnapshot', players);
    await this.syncSkillsToClient(client, body.map);
    const rongThanRaw = await this.redis.get(this.RONG_THAN_KEY);
    if (rongThanRaw) {
      const { userId: ownerUserId, map: rongMap, ngocRongUoc, x, y, gameName } = JSON.parse(rongThanRaw);
      if (rongMap === body.map) {
        const ownerPlayer = players.find(p => p.userId == ownerUserId); 
        client.emit('uocRongThan', { 
          mapToi: true, 
          nguoiUoc: ownerUserId, 
          gameNameNguoiUoc: gameName,
          map: rongMap, 
          x: x,
          y: y,
          ngocRongUoc: ngocRongUoc,
        });
      }
    } else {
      // Key không còn -> reset state client phòng trường hợp client còn state cũ
      client.emit('uocRongThan', {
        mapToi: false,
        nguoiUoc: null,
        gameNameNguoiUoc: null,
        map: body.map,
        x: 0,
        y: 0,
        ngocRongUoc: "",
      });
    }

    client.to(`MAP:${body.map}`).emit('playerSpawn', {
      userId,
      x: body.x,
      y: body.y,
      trangthai: body.trangthai,
      dir: body.dir,
      dau: body.dau,
      than: body.than,
      chan: body.chan,
      timeChoHienBay: body.timeChoHienBay,
      lechDauX: body.lechDauX,
      lechDauY: body.lechDauY,
      lechThanX: body.lechThanX,
      lechThanY: body.lechThanY,
      lechChanX: body.lechChanX,
      lechChanY: body.lechChanY,
      dangMangVanBay: body.dangMangVanBay,
      tenVanBay: body.tenVanBay,
      rong: body.rong,
      cao: body.cao,
      gameName: state.gameName,
      avatar: body.avatar,
      deoLungDung: state.deoLungDung ?? null,
      huyHieuDung: state.huyHieuDung ?? null,
      auraDung: state.auraDung ?? null,
    });
  }


  @SubscribeMessage('player-move')
  async handleMove(
    @ConnectedSocket() client: Socket,
    @MessageBody() body: { x: number, y: number, trangthai: string, dir: number, dau: string, than: string, chan: string, timeChoHienBay: Double, lechDauX: Double, lechDauY: Double, lechThanX: Double, lechThanY: Double, lechChanX: Double, lechChanY: Double, dangMangVanBay: string, tenVanBay: string, rong: Double, cao: Double, avatar: string },
    ) {
    const map = client.data.map;
    const { userId } = client.data.user;

    // Dirty flag pattern: chỉ write DB khi có thay đổi thực sự
    // player-move → SET dirty:{userId} EX 60
    // batch save (20s) → check flag → write → DEL flag
    // Lợi: giảm DB write 60-90% khi player idle
    // Sau này có thể viết 1 event socket để set dirty để client action hành đồng thì dirty luôn

    // TTL 600s (10 phút) — cân bằng giữa 2 yếu tố:
    //
    // 1. DATA DURABILITY (tính đúng đắn dữ liệu):
    //    - Batch save chạy mỗi 20s → trong 600s có 30 lần cơ hội flush
    //    - Nếu server crash, flag vẫn sống đủ lâu để instance mới kịp recover & save
    //    - TTL quá ngắn (vd: 60s) → flag expire trước khi save → mất data
    //
    // 2. MEMORY RECLAMATION (thu hồi bộ nhớ Redis):
    //    - Nếu batch save crash giữa chừng (sau write DB nhưng trước DEL),
    //      flag sẽ tự dọn sau 10p thay vì stuck vĩnh viễn → tránh Redis key leak
    //    - No TTL hoàn toàn: flag zombie tích tụ theo thời gian nếu DEL bị miss
    //
    // Kết luận: 600s = 30x safety margin so với batch interval (20s),
    // đủ dài để đảm bảo data, đủ ngắn để Redis tự dọn rác.

    this.redis.pipeline()
      .set(`dirty:${userId}`, Date.now(), 'EX', 600, 'NX')
      .hset(`GAME:PLAYER:${userId}`, {
        x: body.x,
        y: body.y,
        trangthai: body.trangthai,
        dir: body.dir,
        dau: body.dau,
        than: body.than,
        chan: body.chan,
        timeChoHienBay: body.timeChoHienBay,
        lechDauX: body.lechDauX,
        lechDauY: body.lechDauY,
        lechThanX: body.lechThanX,
        lechThanY: body.lechThanY,
        lechChanX: body.lechChanX,
        lechChanY: body.lechChanY,
        dangMangVanBay: body.dangMangVanBay,
        tenVanBay: body.tenVanBay,
        rong: body.rong,
        cao: body.cao,
        avatar: body.avatar,
      })
      .exec();

    this.server.to(`MAP:${map}`).emit('playerSync', {
      userId,
      x: body.x,
      y: body.y,
      trangthai: body.trangthai,
      dir: body.dir,
      dau: body.dau,
      than: body.than,
      chan: body.chan,
      timeChoHienBay: body.timeChoHienBay,
      lechDauX: body.lechDauX,
      lechDauY: body.lechDauY,
      lechThanX: body.lechThanX,
      lechThanY: body.lechThanY,
      lechChanX: body.lechChanX,
      lechChanY: body.lechChanY,
      dangMangVanBay: body.dangMangVanBay,
      tenVanBay: body.tenVanBay,
      rong: body.rong,
      cao: body.cao,
      avatar: body.avatar,
    });
  }

  /**
   * [use-skill] Xử lý khi user dùng skill
   *
   * Cách 2 (KHÔNG dùng): Gắn vào GAME:PLAYER:${userId} như deo-lung
   *   - HSET field không hỗ trợ TTL per-field → không thể tự expire từng skill
   *   - Phải tự cleanup thủ công khi skill hết thời gian → phức tạp, dễ sót
   *
   * Cách 1 (ĐANG dùng): Tạo key riêng GAME:SKILL:${map}:${userId}:${skillId} với TTL
   *   + Redis tự xóa key khi skill hết thời gian cast → không cần cleanup thủ công
   *   + Sorted Set GAME:SKILL:MAP:${map} (score = expireAt) để syncSkillsToClient
   *     lọc được skill còn hiệu lực khi B join map sau
   *   + Hỗ trợ multi-skill cùng lúc, mỗi skill có TTL độc lập
   */
  @SubscribeMessage('use-skill')
  async handleUseSkill(
    @ConnectedSocket() client: Socket,
    @MessageBody() body: { skillId: string, timeSkill: number }
  ) {
    const { userId } = client.data.user;
    const map = client.data.map;

    if (!body.skillId) return;

    // Set Redis TTL để nếu khi gửi rồi mà user khác mới join map thì vẫn sẽ thấy đang cast Skill

    // TimeSkill Client gửi là giây nên set TTL luôn
    const expireAt = Date.now() + body.timeSkill * 1000;
    const member = `${userId}:${body.skillId}`;

    this.redis.pipeline()
      .set(
        `GAME:SKILL:${map}:${userId}:${body.skillId}`,
        JSON.stringify({ userId, skillId: body.skillId, startedAt: Date.now() }),
        'EX', body.timeSkill
      )
      .zadd(`GAME:SKILL:MAP:${map}`, expireAt, member)
      .exec();

    this.server.to(`MAP:${map}`).emit('useSkill', {
      userId,
      skillId: body.skillId
    });
  }

  @SubscribeMessage('cancel-skill')
  async handleCancelSkill(
    @ConnectedSocket() client: Socket,
    @MessageBody() body: { skillId: string }
  ) {
    const { userId } = client.data.user;
    const map = client.data.map;

    if (!body.skillId) return;

    this.redis.pipeline()
      .del(`GAME:SKILL:${map}:${userId}:${body.skillId}`)
      .zrem(`GAME:SKILL:MAP:${map}`, `${userId}:${body.skillId}`)
      .exec();

    this.server.to(`MAP:${map}`).emit('cancelSkill', {
      userId,
      skillId: body.skillId
    });
  }

  /**
   * [use-deo-lung] Xử lý khi user đeo item lưng
   *
   * Cách 1 (KHÔNG dùng): Tạo key Redis riêng GAME:DEO_LUNG:${map}:${userId}
   *   + Cần syncDeoLungToClient riêng khi B join map
   *   - N user đeo mãi = N key tồn tại vô thời hạn → memory leak
   *   - Phải cleanup thủ công trong handleDisconnect và handleSetMap
   *
   * Cách 2 (ĐANG dùng): Gắn cosmetic thẳng vào GAME:PLAYER:${userId}
   *   + mapSnapshot đã trả về toàn bộ player state → B join sau tự thấy luôn, không cần sync riêng
   *   + Không tạo thêm key Redis nào → không bao giờ leak
   *   + Tự cleanup khi player disconnect vì GAME:PLAYER key đã được xóa rồi
   */
  @SubscribeMessage('use-cosmetic')
  async handleUseCosmetic(
    @ConnectedSocket() client: Socket,
    @MessageBody() body: { field: CosmeticField; value: string }
  ) {
    if (!ALLOWED_COSMETIC_FIELDS.includes(body.field)) return;

    const { userId } = client.data.user;
    const map = client.data.map;

    await this.redis.hset(`GAME:PLAYER:${userId}`, { [body.field]: body.value });

    this.server.to(`MAP:${map}`).emit('useCosmetic', {
      userId,
      field: body.field,
      value: body.value,
    });
  }

  /**
   * [cancel-deo-lung] Xử lý khi user bỏ item lưng
   *
   * Cách 1 (KHÔNG dùng): Xóa key GAME:DEO_LUNG:${map}:${userId} + srem khỏi Set
   *   - Phải nhớ cleanup đúng cả 2 chỗ (key + set), dễ miss → data stale
   *
   * Cách 2 (ĐANG dùng): hdel field cosmetic trong GAME:PLAYER:${userId}
   *   + Chỉ một thao tác duy nhất, không có gì bị sót
   *   + Nhất quán với cách lưu ở use-cosmetic
   */
  @SubscribeMessage('cancel-cosmetic')
  async handleCancelCosmetic(
    @ConnectedSocket() client: Socket,
    @MessageBody() body: { field: CosmeticField }
  ) {
    if (!ALLOWED_COSMETIC_FIELDS.includes(body.field)) return;

    const { userId } = client.data.user;
    const map = client.data.map;

    await this.redis.hdel(`GAME:PLAYER:${userId}`, body.field);

    this.server.to(`MAP:${map}`).emit('cancelCosmetic', { userId, field: body.field });
  }

  @SubscribeMessage('sync-my-state')
  async handleSyncMyState(
    @ConnectedSocket() client: Socket,
    @MessageBody() body: Partial<Record<CosmeticField, string | null>> // Partial biến data thành optional
  ) {
    const { userId } = client.data.user;
    const map = client.data.map;

    for (const field of ALLOWED_COSMETIC_FIELDS) {
      if (!(field in body)) continue;

      const value = body[field];

      if (value) {
        await this.redis.hset(`GAME:PLAYER:${userId}`, { [field]: value });
        this.server.to(`MAP:${map}`).emit('useCosmetic', { userId, field, value });
      } else {
        await this.redis.hdel(`GAME:PLAYER:${userId}`, field);
        this.server.to(`MAP:${map}`).emit('cancelCosmetic', { userId, field });
      }
    }
  }

  @SubscribeMessage('player-chat')
  async handleChat(
    @ConnectedSocket() client: Socket,
    @MessageBody() body: { message: string }
  ) {
    const { userId } = client.data.user;
    const map = client.data.map;

    if (!body.message || body.message.length > 200) return;

    const cleanMessage = censorMessage(body.message);

    this.server.to(`MAP:${map}`).emit('playerChat', {
      userId,
      message: cleanMessage,
    });
  }

  @SubscribeMessage('add-item')
  async handleAddItem(
    @ConnectedSocket() client: Socket,
    @MessageBody() body: { tmpId:number, item: Item }
  ) {
    // console.log('Received add-item:', JSON.stringify(body)); 
    const { userId } = client.data.user;

    if (!body.item) {
      console.log('Item is null/undefined, returning early')
      return;
    }

    const uuid = uuidv4();

    body.item.uuid = uuid;
    body.item.userId = userId;

    this.queueClient.emit('save_item', { data: body.item });

    client.emit('addItem', { tmpId: body.tmpId, uuid: uuid });
  }

  @SubscribeMessage('send-notification')
  async handleNotification(
    @ConnectedSocket() client: Socket,
    @MessageBody() body: { tinNhan: string },
  ) {
    // Room to, gui all User dang online
    client.to(`NotificationGame`).emit('notification', { tinNhan: body.tinNhan });
  }

  private readonly RONG_THAN_KEY = 'GAME:RONG_THAN:ACTIVE'; // value: JSON {userId, map}
  private readonly TIME_DELAY_UOC_RONG = 10 * 60; // 10 phút 
  // private readonly TIME_DELAY_UOC_RONG = 10 ; // 10s

  @SubscribeMessage('uoc-rong-than')
  async handleUocRongThan(
    @ConnectedSocket() client: Socket,
    @MessageBody() body: { ngocRongUoc: string }
  ) {
    const { userId } = client.data.user;
    const map = client.data.map;

    // Lấy tọa độ người ước từ Redis
    const playerState = await this.redis.hgetall(`GAME:PLAYER:${userId}`);

    const UOC_RONG_THAN_SCRIPT = `
      local key    = KEYS[1]
      local value  = ARGV[1]
      local ttl    = tonumber(ARGV[2])

      local existing = redis.call('GET', key)
      if existing then
        local remain = redis.call('TTL', key)
        return {'COOLDOWN', tostring(remain)}
      end

      redis.call('SET', key, value, 'EX', ttl)
      return {'OK', '0'}
    `;

    const value = JSON.stringify({ 
      userId, 
      map, 
      ngocRongUoc: body.ngocRongUoc,
      x: Number(playerState.x),
      y: Number(playerState.y),
      gameName: playerState.gameName,
    });

    const [status, remain] = await this.redis.eval(
      UOC_RONG_THAN_SCRIPT,
      1,
      this.RONG_THAN_KEY,
      value,
      String(this.TIME_DELAY_UOC_RONG),
    ) as [string, string];

    if (status === 'COOLDOWN') {
      const minutesLeft = Math.ceil(Number(remain) / 60);
      client.emit('uocRongThanResult', {
        duocGoiRong: false,
        message: `Ngọc rồng cần khôi phục trong ${minutesLeft} phút nữa`,
      });
      return;
    }

    client.emit('uocRongThanResult', {
      duocGoiRong: true,
      message: 'OK',
    });

    this.server.to(`MAP:${map}`).emit('uocRongThan', {
      mapToi: true,
      nguoiUoc: userId,
      gameNameNguoiUoc: playerState.gameName,
      map: map, // Gửi thêm map để tránh user đã chuyển map mới khi gói tin chưa kịp tới
      x: Number(playerState.x),
      y: Number(playerState.y),
      ngocRongUoc: body.ngocRongUoc,
    });
  }

  @SubscribeMessage('uoc-xong')
  async handleUocXong(
    @ConnectedSocket() client: Socket,
    @MessageBody() body: {}
  ) {
    const { userId } = client.data.user;
    const map = client.data.map;

    const raw = await this.redis.get(this.RONG_THAN_KEY);
    if (!raw) {
      // Key đã expire, cron có thể chưa kịp emit → emit reset cả map luôn cho chắc
      this.server.to(`MAP:${map}`).emit('uocRongThan', {
        mapToi: false,
        nguoiUoc: userId,
        gameNameNguoiUoc: null,
        map: map,
        x: 0,
        y: 0,
        ngocRongUoc: "",
      });
      // Xóa snapshot để cron không emit trùng lần nữa
      await this.redis.del(this.RONG_THAN_SNAPSHOT_KEY);
      return;
    }

    const { userId: ownerUserId, mapRedis } = JSON.parse(raw);
    if (String(ownerUserId) !== String(userId)) {
      client.emit(`Game:${userId}`, { success: false, message: 'Bạn không phải người triệu hồi rồng thần' });
      return;
    }

    await this.redis.del(this.RONG_THAN_KEY);

    this.server.to(`MAP:${map}`).emit('uocRongThan', {
      mapToi: false,
      nguoiUoc: userId,
      gameNameNguoiUoc: null,
      map: mapRedis,
      x: 0,
      y: 0,
      ngocRongUoc: "",
    });
  }

  private readonly RONG_THAN_SNAPSHOT_KEY = 'GAME:RONG_THAN:SNAPSHOT';
  // Poll mỗi 5 phút để detect key rồng thần expire (user crash hoặc không ước).
  // Cần RONG_THAN_SNAPSHOT_KEY vì:
  //   - Key expire rồi thì không đọc được {userId, map} nữa → snapshot giữ lại map để emit đúng chỗ
  //   - Không có snapshot thì raw=null sẽ spam emit reset mỗi 5p mãi mãi
  //   - Snapshot lưu Redis thay vì memory để tránh bug khi instance giữ snapshot bị chết,
  //     instance mới acquire lock vẫn đọc được
  // Flow: raw!=null → cập nhật snapshot
  //       raw=null && snapshot!=null → key vừa expire, emit reset đúng map, xóa snapshot
  //       raw=null && snapshot=null  → không có gì, bỏ qua
  @Cron(CronExpression.EVERY_5_MINUTES)
  async handleRongThanExpiryCron() {
    let lock: RLock | null = null;
    try {
      lock = await this.redlock.acquire(['lock:cron:rongThanExpiry'], 10_000);

      const raw = await this.redis.get(this.RONG_THAN_KEY);

      if (raw) {
        await this.redis.set(this.RONG_THAN_SNAPSHOT_KEY, raw);
      } else {
        const snapshot = await this.redis.get(this.RONG_THAN_SNAPSHOT_KEY);
        if (snapshot) {
          const { map } = JSON.parse(snapshot);
          this.server.to(`MAP:${map}`).emit('uocRongThan', { mapToi: false, nguoiUoc: null,gameNameNguoiUoc: null, map: map, x: 0, y: 0, ngocRongUoc: "" });
          await this.redis.del(this.RONG_THAN_SNAPSHOT_KEY);
        }
      }
    } catch (err) {
      if (err instanceof ExecutionError || err instanceof ResourceLockedError) {
        console.warn('Cron rongThan bị lock bởi instance khác, bỏ qua');
        return;
      }
      throw err;
    } finally {
      if (lock) {
        await lock.release();
      }
    }
  }

  // TODO: 1, Thêm 1 api gửi items Id vào để check xem đúng người sở hữu item đó không 
  //       2, Thêm 1 api gửi items Id và nhận lại các thông số item để render thông tin item ở client 
  // Gọi khi User A muốn gửi yêu cầu giao dịch cho User B
  @SubscribeMessage('trade:request')
  async handleTradeItem(
    @ConnectedSocket() client: Socket,
    @MessageBody() body: { targetId: number },
  ) {
    const userId = client.data.user.userId;
    const state = await this.redis.hgetall(`GAME:PLAYER:${userId}`);
    if (!state) return;
    client.to(`Game:${body.targetId}`).emit('trade:request', { fromUserId: userId });
  }

  // Gọi khi User B accept yêu cầu giao dịch của User A
  @SubscribeMessage('trade:accept')
  async tradeAccept(
    @ConnectedSocket() client: Socket,
    @MessageBody() body: { fromUserId: number },
  ) {
    const userId = client.data.user.userId;

    // set trạng thái giao dịch trong Redis
    const mySession = await this.redis.get(`GAME:TRADE:SESSION:${userId}`);
    const otherSession = await this.redis.get(`GAME:TRADE:SESSION:${body.fromUserId}`);
    if (mySession || otherSession) {
      // có thể emit lỗi cho client nếu muốn
      return;
    }

    const sessionId = userId < body.fromUserId ? `${userId}:${body.fromUserId}` : `${body.fromUserId}:${userId}`;

    await this.redis
      .multi()
      .set(`GAME:TRADE:SESSION:${userId}`, sessionId, 'EX', 300)
      .set(`GAME:TRADE:SESSION:${body.fromUserId}`, sessionId, 'EX', 300)
      .set(`GAME:TRADE:STATE:${sessionId}`, 'OPEN', 'EX', 300)
      .exec();

    // server quyết định mở giao dịch
    // Gửi cho cả 2 để cả 2 hiện popup giao dịch
    this.server.to(`Game:${userId}`).emit('trade:open', { with: body.fromUserId });
    this.server.to(`Game:${body.fromUserId}`).emit('trade:open', { with: userId });

  }

  // Gọi event khi 1 trong 2 hủy giao dịch
  @SubscribeMessage('trade:cancel')
  async tradeCancel(
    @ConnectedSocket() client: Socket,
    @MessageBody() body: { withUserId: number },
  ) {
    const userId = client.data.user.userId;
    const withUserId = body.withUserId;

    const sessionId =
      userId < withUserId ? `${userId}:${withUserId}` : `${withUserId}:${userId}`;

    const mySession = await this.redis.get(`GAME:TRADE:SESSION:${userId}`);
    if (mySession !== sessionId) return; // fake packet hoặc trade khác

    await this.redis.multi()
      .del(`GAME:TRADE:SESSION:${userId}`)
      .del(`GAME:TRADE:SESSION:${withUserId}`)
      .del(`GAME:TRADE:STATE:${sessionId}`)
      .del(`GAME:TRADE:OFFER:${sessionId}:${userId}`)
      .del(`GAME:TRADE:OFFER:${sessionId}:${withUserId}`)
      .del(`GAME:TRADE:LOCK:${sessionId}:${userId}`)
      .del(`GAME:TRADE:LOCK:${sessionId}:${withUserId}`)
      .del(`GAME:TRADE:CONFIRM:${sessionId}:${userId}`)
      .del(`GAME:TRADE:CONFIRM:${sessionId}:${withUserId}`)
      .exec();

    // Gửi cho cả 2 để tắt popup và hiện thông báo gd bị hủy ( thông báo pet ở client )
    this.server.to(`Game:${withUserId}`).emit('trade:cancelled', { by: userId });
    this.server.to(`Game:${userId}`).emit('trade:cancelled', { by: userId });
    this.server.to(`Game:${withUserId}`).emit('notification', { tinNhan: "Giao dịch đã bị hủy" });
    this.server.to(`Game:${userId}`).emit('notification', { tinNhan: "Giao dịch đã bị hủy" });
  }

  @SubscribeMessage('trade:offer:add')
  async tradeOfferAdd(
    @ConnectedSocket() client: Socket,
    @MessageBody() body: { withUserId: number; itemUuid: string },
  ) {
    const userId = client.data.user.userId;
    const { withUserId, itemUuid } = body;

    let sessionId: string;
    let state: string;

    try {
      ({ sessionId, state } = await this.getValidSession(userId, withUserId));
    } catch {
      return;
    }

    if (state !== 'OPEN') return;

    const locked = await this.redis.get(`GAME:TRADE:LOCK:${sessionId}:${userId}`);
    if (locked) return;

    const key = `GAME:TRADE:OFFER:${sessionId}:${userId}`;
    const current = JSON.parse((await this.redis.get(key)) || '[]');

    // Tránh add trùng ngay tại server
    if (current.some(i => i.itemUuid === itemUuid)) return;

    current.push({ itemUuid });
    await this.redis.set(key, JSON.stringify(current), 'EX', 300);

    // Chỉ gửi đúng 1 item mới + action
    this.server.to(`Game:${withUserId}`).emit('trade:offer:update', {
      from: userId,
      action: 'add',
      itemUuid,
    });
  }

  @SubscribeMessage('trade:offer:remove')
  async tradeOfferRemove(
    @ConnectedSocket() client: Socket,
    @MessageBody() body: { withUserId: number; itemUuid: string },
  ) {
    const userId = client.data.user.userId;
    const { withUserId, itemUuid } = body;

    let sessionId: string;
    let state: string;

    try {
      ({ sessionId, state } = await this.getValidSession(userId, withUserId));
    } catch {
      return;
    }

    if (state !== 'OPEN') return;

    const locked = await this.redis.get(`GAME:TRADE:LOCK:${sessionId}:${userId}`);
    if (locked) return;

    const key = `GAME:TRADE:OFFER:${sessionId}:${userId}`;
    const current = JSON.parse((await this.redis.get(key)) || '[]');
    const next = current.filter(i => i.itemUuid !== itemUuid);

    await this.redis.set(key, JSON.stringify(next), 'EX', 300);

    // Remove không cần gửi data item, client tự xóa theo uuid
    this.server.to(`Game:${withUserId}`).emit('trade:offer:update', {
      from: userId,
      action: 'remove',
      itemUuid,
    });
  }

  @SubscribeMessage('trade:lock')
  async tradeLock(
    @ConnectedSocket() client: Socket,
    @MessageBody() body: { withUserId: number },
  ) {
    const TRADE_LOCK_SCRIPT = `
      local sessionId  = KEYS[1]
      local userId     = KEYS[2]
      local withUserId = KEYS[3]

      local lockMe    = 'GAME:TRADE:LOCK:' .. sessionId .. ':' .. userId
      local lockOther = 'GAME:TRADE:LOCK:' .. sessionId .. ':' .. withUserId
      local stateKey  = 'GAME:TRADE:STATE:' .. sessionId

      redis.call('SET', lockMe, '1', 'EX', 300)

      if not redis.call('GET', lockOther) then
        return 'WAIT'
      end

      redis.call('SET', stateKey, 'LOCKED', 'EX', 300)
      return 'BOTH_LOCKED'
    `;

    const userId = client.data.user.userId;

    let sessionId: string;
    let state: string;
    try {
      ({ sessionId, state } = await this.getValidSession(userId, body.withUserId));
    } catch (e) {
      return;
    }

    if (state !== 'OPEN' && state !== 'LOCKED') {
      return;
    }

    const rawResult = await this.redis.eval(
      TRADE_LOCK_SCRIPT,
      3,
      sessionId,
      String(userId),
      String(body.withUserId),
    ) as string;

    const status = rawResult.split('|')[0];

    if (status === 'WAIT') return;
    this.server.to(`Game:${userId}`).emit('trade:bothLocked', { ok: true });
    this.server.to(`Game:${body.withUserId}`).emit('trade:bothLocked', { ok: true });
  }

  // Sau khi cả 2 ấn khóa, sẽ tự gọi event này
  @SubscribeMessage('trade:check')
  async tradeCheck(
    @ConnectedSocket() client: Socket,
    @MessageBody() body: { withUserId: number, oConTrongBanThan: number },
  ) {
    const userId = client.data.user.userId;
    const withUserId = body.withUserId;

    let sessionId: string;
    let state: string;

    try {
      ({ sessionId, state } = await this.getValidSession(userId, withUserId));
    } catch {
      return;
    }

    if (state !== 'LOCKED') return;

    // Lấy danh sách item mà user sẽ NHẬN
    const otherOfferKey = `GAME:TRADE:OFFER:${sessionId}:${withUserId}`;
    const otherOffer = JSON.parse((await this.redis.get(otherOfferKey)) || '[]');

    const soItemSeNhan = otherOffer.length;


    if (body.oConTrongBanThan < soItemSeNhan) {
      // huỷ giao dịch cho cả 2
      await this.redis.set(`GAME:TRADE:STATE:${sessionId}`, 'CANCELLED', 'EX', 30);

      await this.redis.multi()
        .del(`GAME:TRADE:SESSION:${userId}`)
        .del(`GAME:TRADE:SESSION:${withUserId}`)
        .del(`GAME:TRADE:STATE:${sessionId}`)
        .del(`GAME:TRADE:OFFER:${sessionId}:${userId}`)
        .del(`GAME:TRADE:OFFER:${sessionId}:${withUserId}`)
        .del(`GAME:TRADE:LOCK:${sessionId}:${userId}`)
        .del(`GAME:TRADE:LOCK:${sessionId}:${withUserId}`)
        .del(`GAME:TRADE:CONFIRM:${sessionId}:${userId}`)
        .del(`GAME:TRADE:CONFIRM:${sessionId}:${withUserId}`)
        .exec();

      this.server.to(`Game:${userId}`).emit('trade:cancelled', { by: userId });
      this.server.to(`Game:${withUserId}`).emit('trade:cancelled', { by: userId });

      this.server.to(`Game:${userId}`).emit('notification', {
        tinNhan: 'Hành trang không đủ chỗ trống để nhận đồ',
      });
      this.server.to(`Game:${withUserId}`).emit('notification', {
        tinNhan: 'Đối phương không đủ chỗ trống trong hành trang',
      });

      return;
    }

    // Đánh dấu user này đã CHECK OK
    await this.redis.set(
      `GAME:TRADE:CHECK_OK:${sessionId}:${userId}`,
      1,
      'EX',
      120,
    );

    const otherChecked = await this.redis.get(
      `GAME:TRADE:CHECK_OK:${sessionId}:${withUserId}`,
    );

    // Khi cả 2 đều OK → cho phép confirm
    if (otherChecked) {
      this.server.to(`Game:${userId}`).emit('trade:check:ok');
      this.server.to(`Game:${withUserId}`).emit('trade:check:ok');
    }
  }

  // Sau khi đầy đủ điều kiện có thể confirm giao dịch
  @SubscribeMessage('trade:confirm')
  async tradeConfirm(
    @ConnectedSocket() client: Socket,
    @MessageBody() body: { withUserId: number },
  ) {
    const TRADE_CONFIRM_SCRIPT = `
      local sessionId    = KEYS[1]
      local userId       = KEYS[2]
      local withUserId   = KEYS[3]

      local confirmMe    = 'GAME:TRADE:CONFIRM:'  .. sessionId .. ':' .. userId
      local confirmOther = 'GAME:TRADE:CONFIRM:'  .. sessionId .. ':' .. withUserId
      local checkMe      = 'GAME:TRADE:CHECK_OK:' .. sessionId .. ':' .. userId
      local checkOther   = 'GAME:TRADE:CHECK_OK:' .. sessionId .. ':' .. withUserId
      local executingKey = 'GAME:TRADE:EXECUTING:' .. sessionId
      local offerMe      = 'GAME:TRADE:OFFER:'    .. sessionId .. ':' .. userId
      local offerOther   = 'GAME:TRADE:OFFER:'    .. sessionId .. ':' .. withUserId

      redis.call('SET', confirmMe, '1', 'EX', 300)

      if not redis.call('GET', confirmOther) then
        return 'WAIT'
      end

      if not redis.call('GET', checkMe) or not redis.call('GET', checkOther) then
        return 'NOT_READY'
      end

      if not redis.call('SET', executingKey, '1', 'EX', 30, 'NX') then
        return 'LOCKED'
      end

      local offerMeData = redis.call('GET', offerMe)
      if not offerMeData then offerMeData = '[]' end

      local offerOtherData = redis.call('GET', offerOther)
      if not offerOtherData then offerOtherData = '[]' end

      redis.call('DEL',
        'GAME:TRADE:SESSION:'  .. userId,
        'GAME:TRADE:SESSION:'  .. withUserId,
        'GAME:TRADE:STATE:'    .. sessionId,
        offerMe,
        offerOther,
        'GAME:TRADE:LOCK:'     .. sessionId .. ':' .. userId,
        'GAME:TRADE:LOCK:'     .. sessionId .. ':' .. withUserId,
        confirmMe,
        confirmOther,
        executingKey
      )

      return offerMeData .. '|' .. offerOtherData
    `;
    const userId = client.data.user.userId;

    let sessionId: string;
    let state: string;
    try {
      ({ sessionId, state } = await this.getValidSession(userId, body.withUserId));
    } catch {
      return;
    }

    if (state !== 'LOCKED') return;

    const result = await this.redis.eval(
      TRADE_CONFIRM_SCRIPT,
      3,
      sessionId,
      String(userId),
      String(body.withUserId),
    ) as string;

    if (result === 'WAIT') {
      this.server.to(`Game:${userId}`).emit('notification', { tinNhan: 'Vui lòng đợi đối phương xác nhận' });
      return;
    }

    if (result === 'NOT_READY' || result === 'LOCKED') return;

    // result = '[...offerMe]|[...offerOther]'
    const separatorIndex = result.indexOf('|');
    const offerMe    = JSON.parse(result.substring(0, separatorIndex));
    const offerOther = JSON.parse(result.substring(separatorIndex + 1));

    this.queueClient.emit('swap', {
      offers: [
        { itemUuids: offerMe,    swap_userId: body.withUserId },
        { itemUuids: offerOther, swap_userId: userId },
      ],
    });

    this.server.to(`Game:${userId}`).emit('trade:success');
    this.server.to(`Game:${body.withUserId}`).emit('trade:success');
    this.server.to(`Game:${userId}`).emit('notification', { tinNhan: 'Giao dịch thành công' });
    this.server.to(`Game:${body.withUserId}`).emit('notification', { tinNhan: 'Giao dịch thành công' });

    /**
    * Atomic check-and-execute cho trade confirm dùng Redis Lua script.
    *
    * TẠI SAO CẦN LUA:
    * Khi cả 2 user confirm gần như cùng lúc, nếu dùng GET/SET riêng lẻ sẽ xảy ra race condition:
    *   - User A: SET confirmA → GET confirmB (thấy có) → pass check → chạy swap
    *   - User B: SET confirmB → GET confirmA (thấy có) → pass check → chạy swap
    *   → Cả 2 cùng chạy swap → item bị swap 2 lần hoặc lỗi data
    *
    * Lua script chạy ATOMIC trên Redis (single-threaded), toàn bộ logic
    * từ SET confirm → check → acquire executing lock → lấy data → cleanup
    * xảy ra trong 1 operation duy nhất, Redis không xử lý command nào khác ở giữa.
    *
    * EXECUTING LOCK (SET NX):
    * Dù 2 user gọi đồng thời và đều pass hết các check,
    * chỉ đúng 1 script thắng được SET NX → script kia return 'LOCKED' và dừng.
    * Đây là lớp bảo vệ cuối cùng đảm bảo swap chỉ chạy đúng 1 lần.
    *
    * CÁC RETURN VALUE:
    *   'WAIT'      → Người kia chưa confirm, chờ
    *   'NOT_READY' → Chưa đủ điều kiện (thiếu CHECK_OK)
    *   'LOCKED'    → Người kia đã acquire lock và đang thực thi, bỏ qua
    *   '<offerMe>|<offerOther>' → Thắng lock, kèm data offer để đẩy vào queue swap
    */
  }


  private getSessionId(a: number, b: number) {
    return a < b ? `${a}:${b}` : `${b}:${a}`;
  }

  private async getValidSession(userId: number, withUserId: number) {
    const sessionId = this.getSessionId(userId, withUserId);

    const mySession = await this.redis.get(`GAME:TRADE:SESSION:${userId}`);
    if (mySession !== sessionId) throw new Error('INVALID_SESSION');

    const state = await this.redis.get(`GAME:TRADE:STATE:${sessionId}`);
    if (!state) throw new Error('NO_TRADE');

    return { sessionId, state };
  }

  async getPlayersInMap(map: string) {
    const userIds = await this.redis.smembers(`GAME:MAP:${map}`);
    if (!userIds.length) return [];

    const pipeline = this.redis.pipeline();

    userIds.forEach(id => {
      pipeline.hgetall(`GAME:PLAYER:${id}`);
    });

    const results = await pipeline.exec();
    if (!results) return [];

    return results.map((result, index) => {
      const [err, state] = result;

      if (err || !state) return null;

      const playerState = state as Record<string, string>;

      return {
        userId: Number(userIds[index]),
        x: Number(playerState.x),
        y: Number(playerState.y),
        trangthai: playerState.trangthai ?? 'DUNG_YEN',
        dir: Number(playerState.dir ?? 1),
        dau: playerState.dau,
        than: playerState.than,
        chan: playerState.chan,
        timeChoHienBay: playerState.timeChoHienBay,
        lechDauX: playerState.lechDauX,
        lechDauY: playerState.lechDauY,
        lechThanX: playerState.lechThanX,
        lechThanY: playerState.lechThanY,
        lechChanX: playerState.lechChanX,
        lechChanY: playerState.lechChanY,
        dangMangVanBay: playerState.dangMangVanBay,
        tenVanBay: playerState.tenVanBay,
        rong: playerState.rong,
        cao: playerState.cao,
        gameName: playerState.gameName,
        avatar: playerState.avatar,
        deoLungDung: playerState.deoLungDung ?? "",
        huyHieuDung: playerState.huyHieuDung ?? "",
        auraDung: playerState.auraDung ?? "",
      };
    }).filter(Boolean);
  }

  // @OnEvent('auth.revoke_all_token')
  async handleRevokeAllToken(userId: number) {
    await this.kickSocket(userId);
    await this.redis.del(`user:${userId}:gameSession`); // xóa session 
    // Sau này cần implements case user giả tắt mạng 1-2s để lách event kick + handle kết nối lại thì viết thêm interceptor check xem có gameSession thật không, không có thì kick phát nữa
    // Case này cần xóa session cho clean redis thôi ( và tránh tình trạng data ảo, hoặc user đăng nhập lại )
  }

  // @OnEvent('auth.kick_socket')
  async handleKickSocket(userId: number) {
    await this.kickSocket(userId); // chỉ kick, không xóa session 
    // Xóa sẽ dẫn đến conflict vì sẽ xóa gameSession mới vừa set luôn
  }

  async kickSocket(userId: number) {
    this.server.to(`Game:${userId}`).emit('force_logout', {
      message: 'Tài khoản đăng nhập ở nơi khác',
    });
    
    // Khác setTimeout thường là nó chặn dòng sau ( có await nên phải data trả về là Promise )
    await new Promise(resolve => setTimeout(resolve, 100));
    // Disconnect socket qua adapter (Socket.IO hỗ trợ sẵn)
    this.server.in(`Game:${userId}`).disconnectSockets(true);
  }

  private async syncSkillsToClient(client: Socket, map: string) {
    const now = Date.now();

    // Clean up luôn
    await this.redis.zremrangebyscore(`GAME:SKILL:MAP:${map}`, '-inf', now);

    // members = ["userId1:skillA", "userId1:skillB", "userId2:skillC"]
    const members = await this.redis.zrangebyscore(`GAME:SKILL:MAP:${map}`, now, '+inf');
    if (!members.length) return;

    const pipeline = this.redis.pipeline();
    members.forEach(member => {
      const [userId, skillId] = member.split(':');
      pipeline.get(`GAME:SKILL:${map}:${userId}:${skillId}`);
    });

    const results = await pipeline.exec();
    if (!results) return;

    const skills = results
      .map(([err, raw]) => (!err && raw ? JSON.parse(raw as string) : null))
      .filter(Boolean);

    client.emit('syncSkills', skills);
  }

  
  // Client web mua đồ/nạp tiền -> gọi hàm này để gửi thông báo cho client game
  // Client game call api lấy data mới vào thông báo ra màn hình
  // Sử dụng EventEmitter vì sau này có thể mở rộng thêm, và chỉ cần lắng nghe là đc pub/sub thay vì viết thêm logic vào hàm addItem/addNgoc/addVang
  // @OnEvent('user.nap_tien')
  async handleNapTien(event: NapTienEvent) {
    if (event.type === LoaiNapTien.ITEM) {
      const { userId, itemId, quantity = 1 } = event;

      const itemName = getItemName(itemId);

      this.server.to(`Game:${userId}`).emit('notification', {
        type: 'NAP_TIEN',
        data: {
          loai: event.type,
          itemId,
          quantity
        },
        tinNhan: `Bạn vừa mua\nx${quantity} ${itemName}\ntừ web`
      });

    } else {
      const { userId, amount } = event;

      this.server.to(`Game:${userId}`).emit('notification', {
        type: 'NAP_TIEN',
        data: {
          loai: event.type,
          soLuong: amount
        },
        tinNhan: `Bạn vừa mua\n${amount} ${event.type.toLowerCase()}\ntừ web`
      });
    }
  }
}

function censorMessage(message: string): string {
  const BAD_WORDS = ['dm', 'đm', 'vcl', 'cc', 'lol'];
  let result = message;

  for (const word of BAD_WORDS) {
    const regex = new RegExp(word, 'gi');
    result = result.replace(regex, '*'.repeat(word.length));
  }

  return result;
}

function getItemName(itemId: number): string {
  switch (itemId) {
    case 1: return "Cải trang Super Black Goku";
    case 2: return "Trứng đệ tử";
    case 3: return "Áo vải thô";
    case 4: return "Quần thần linh";
    case 5: return "Găng vải thô";
    case 6: return "Giày vải thô";
    case 7: return "Nhẫn thần linh";
    default: return `Item #${itemId}`;
  }
}
