import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:PiliPlus/http/init.dart';
import 'package:PiliPlus/services/logger.dart';
import 'package:flutter/foundation.dart' show kDebugMode;
import 'package:flutter_smart_dialog/flutter_smart_dialog.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

class PackageHeader {
  final int protocolVer;
  final int operationCode;
  final int seq;

  @override
  String toString() {
    return 'PackageHeader{protocolVer: $protocolVer, operationCode: $operationCode, seq: $seq}';
  }

  const PackageHeader({
    required this.protocolVer,
    required this.operationCode,
    required this.seq,
  });

  Uint8List toBytes(int contentSize) {
    final bytes = ByteData(0x10)
      ..setInt32(0, 0x10 + contentSize, Endian.big)
      ..setInt16(4, 0x10, Endian.big)
      ..setInt16(6, protocolVer, Endian.big)
      ..setInt32(8, operationCode, Endian.big)
      ..setInt32(12, seq, Endian.big);
    return bytes.buffer.asUint8List();
  }
}

class PackageHeaderRes extends PackageHeader {
  PackageHeaderRes({
    required this.totalSize,
    required this.headerSize,
    required super.protocolVer,
    required super.operationCode,
    required super.seq,
  });
  final int totalSize;
  final int headerSize;

  static PackageHeaderRes? fromBytesData(Uint8List data) {
    if (data.length < 10) {
      logger.w('数据不足以解析PackageHeader');
      return null;
    }
    final byteData = ByteData.sublistView(data);

    final totalSize = byteData.getUint32(0, Endian.big);
    final headerSize = byteData.getUint16(4, Endian.big);
    final protocolVer = byteData.getUint16(6, Endian.big);
    final operationCode = byteData.getUint32(8, Endian.big);
    final seq = byteData.getUint32(12, Endian.big);

    return PackageHeaderRes(
      totalSize: totalSize,
      headerSize: headerSize,
      protocolVer: protocolVer,
      operationCode: operationCode,
      seq: seq,
    );
  }

  @override
  String toString() {
    return 'PackageHeaderRes{totalSize: $totalSize, headerSize: $headerSize, protocolVer: $protocolVer, operationCode: $operationCode, seq: $seq}';
  }
}

abstract class Message {
  String toJsonStr();
}

class AuthMessage implements Message {
  int roomid;
  int uid;
  int protover;
  String platform;
  int type;
  String key;

  AuthMessage({
    required this.roomid,
    required this.uid,
    required this.protover,
    required this.platform,
    required this.type,
    required this.key,
  });

  @override
  String toJsonStr() {
    final message = {
      'roomid': roomid,
      'uid': uid,
      'protover': protover,
      'platform': platform,
      'type': type,
      'key': key,
    };
    return jsonEncode(message);
  }
}

abstract class AbstractPackage<T> {
  PackageHeader header;
  T body;
  Uint8List marshal();
  AbstractPackage({required this.header, required this.body});
}

//认证包
class AuthPackage extends AbstractPackage<Message> {
  AuthPackage({required super.header, required super.body});

  @override
  Uint8List marshal() {
    final json = utf8.encode(body.toJsonStr());
    final buffer = BytesBuilder()
      ..add(header.toBytes(json.length))
      ..add(json);
    return buffer.toBytes();
  }
}

//心跳包
class HeartbeatPackage extends AbstractPackage<dynamic> {
  HeartbeatPackage({required super.header, super.body});

  @override
  Uint8List marshal() {
    return header.toBytes(0);
  }
}

class LiveMessageStream {
  String streamToken;
  int roomId, uid;
  List<String> servers;
  final List<void Function(dynamic obj)> _eventListeners = [];
  LiveMessageStream({
    required this.streamToken,
    required this.roomId,
    required this.uid,
    required this.servers,
  });

  static final _zlib = ZLibDecoder();

  bool _active = true;
  bool _closed = false;
  bool _connecting = false;
  int _retry = 0;
  bool _waitingAck = false;
  Timer? _reconnectTimer;
  WebSocketChannel? _channel;
  StreamSubscription? _socketSubscription;
  Timer? _timer;
  static const String logTag = "LiveStreamService";

  Future<void> init() async {
    // 单飞：已经连着或正在拨号时不重复发起
    if (_closed || _connecting || _channel != null) return;
    _connecting = true;
    final authPackage = AuthPackage(
      header: const PackageHeader(
        protocolVer: 1,
        operationCode: 7,
        seq: 1,
      ),
      body: AuthMessage(
        roomid: roomId,
        uid: uid,
        protover: 3,
        platform: 'web',
        type: 2,
        key: streamToken,
      ),
    );

    // final marshaledData = authPackage.marshal();
    // logger.d(marshaledData);
    try {
      Future<WebSocketChannel> getSocket() async {
        for (final server in servers) {
          try {
            final channel = WebSocketChannel.connect(Uri.parse(server));
            await channel.ready;
            return channel;
          } catch (_) {}
        }
        throw Exception("all servers connect failed");
      }

      final ch = await getSocket();
      if (_closed) {
        await ch.sink.close();
        return;
      }
      _channel = ch;
      // 回调带连接身份：旧连接的迟到事件不能关掉新连接
      _socketSubscription = ch.stream.listen(
        onData,
        onDone: () {
          if (identical(_channel, ch)) _onSocketClosed();
        },
        onError: (_) {
          if (identical(_channel, ch)) _onSocketClosed();
        },
      );
      _waitingAck = false;
      ch.sink.add(authPackage.marshal());
    } catch (e) {
      if (kDebugMode) logger.i("$logTag init failed: $e");
      if (_retry == 0 && !_closed) {
        SmartDialog.showToast("弹幕地址链接失败: $e");
      }
      _onSocketClosed();
    } finally {
      _connecting = false;
    }
  }

  /// 连接断开 / 认证失败 / 心跳无回应时进入：拆掉旧连接并按指数退避重连。
  void _onSocketClosed() {
    if (_closed) return;
    _waitingAck = false;
    _timer?.cancel();
    _timer = null;
    _socketSubscription?.cancel();
    _socketSubscription = null;
    _channel?.sink.close();
    _channel = null;
    _retry = _retry >= 5 ? 5 : _retry + 1;
    final delay = Duration(seconds: 1 << _retry);
    _reconnectTimer?.cancel();
    if (kDebugMode) {
      logger.i("$logTag reconnect in ${delay.inSeconds}s (#$_retry)");
    }
    _reconnectTimer = Timer(delay, () {
      if (_closed) return;
      init();
    });
  }

  @pragma('vm:notify-debugger-on-exception')
  void _processingData(Uint8List data) {
    try {
      final subHeader = PackageHeaderRes.fromBytesData(data);
      if (subHeader != null) {
        if (_eventListeners.isNotEmpty) {
          final msgBody = jsonDecode(
            utf8.decode(
              Uint8List.sublistView(
                data,
                subHeader.headerSize,
                subHeader.totalSize,
              ),
            ),
          );
          for (final f in _eventListeners) {
            f(msgBody);
          }
        }
        if (subHeader.totalSize < data.length) {
          _processingData(Uint8List.sublistView(data, subHeader.totalSize));
        }
      }
    } catch (_) {}
  }

  Future<void> _heartBeat() async {
    if (!_active) {
      if (kDebugMode) logger.i("$logTag init heartBeat inactive $hashCode");
      close();
      return;
    }
    if (kDebugMode) logger.i("$logTag 直播间信息流认证成功 $hashCode");
    _retry = 0;
    int heartBeatCount = 1;
    _timer ??= Timer.periodic(const Duration(seconds: 30), (timer) {
      if (!_active) {
        if (kDebugMode) logger.i("$logTag heartBeat inactive $hashCode");
        timer.cancel();
        close();
        return;
      }
      if (_waitingAck) {
        // 上一拍心跳没等到 op=3 回包：连接已半死（TCP 未断但流已停），重连
        if (kDebugMode) logger.i("$logTag heartBeat ack timeout $hashCode");
        timer.cancel();
        _onSocketClosed();
        return;
      }
      _waitingAck = true;
      if (kDebugMode) logger.i("$logTag heartBeat $hashCode");
      final package = HeartbeatPackage(
        header: PackageHeader(
          protocolVer: 1,
          operationCode: 2,
          seq: heartBeatCount,
        ),
      );
      try {
        _channel?.sink.add(package.marshal());
      } catch (_) {
        timer.cancel();
        _onSocketClosed();
        return;
      }
      heartBeatCount++;
    });
  }

  void addEventListener(void Function(dynamic) func) {
    _eventListeners.add(func);
  }

  @pragma('vm:notify-debugger-on-exception')
  void onData(dynamic data) {
    final header = PackageHeaderRes.fromBytesData(data as Uint8List);
    if (header != null) {
      //心跳包回复不用处理
      if (header.operationCode == 3) {
        _waitingAck = false;
        return;
      }
      if (header.operationCode == 8) {
        _heartBeat();
      }
      final List<int> decompressedData;
      try {
        switch (header.protocolVer) {
          case 0:
          case 1:
            _processingData(data);
            return;
          case 2:
            decompressedData = _zlib.convert(Uint8List.sublistView(data, 0x10));
            break;
          case 3:
            decompressedData = Request.brotli.convert(
              Uint8List.sublistView(data, 0x10),
            );
          //debugPrint('Body: ${utf8.decode()}');
          default:
            return;
        }
        _processingData(
          decompressedData is Uint8List
              ? decompressedData
              : Uint8List.fromList(decompressedData),
        );
      } catch (_) {}
    }
  }

  void close() {
    _active = false;
    _closed = true;
    _waitingAck = false;
    if (kDebugMode) logger.i("$logTag close $hashCode");
    _reconnectTimer?.cancel();
    _reconnectTimer = null;
    _timer?.cancel();
    _timer = null;
    _eventListeners.clear();
    _socketSubscription?.cancel();
    _socketSubscription = null;
    _channel?.sink.close();
    _channel = null;
  }
}
