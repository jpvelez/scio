package org.apache.beam.examples;// automatically generated by the FlatBuffers compiler, do not modify

import java.nio.*;
import java.lang.*;

import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class Endsong extends Table {
    public static Endsong getRootAsEndsong(ByteBuffer _bb) { return getRootAsEndsong(_bb, new Endsong()); }
    public static Endsong getRootAsEndsong(ByteBuffer _bb, Endsong obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
    public void __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; }
    public Endsong __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

    public String userId() { int o = __offset(4); return o != 0 ? __string(o + bb_pos) : null; }
    public ByteBuffer userIdAsByteBuffer() { return __vector_as_bytebuffer(4, 1); }
    public String playTrack() { int o = __offset(6); return o != 0 ? __string(o + bb_pos) : null; }
    public ByteBuffer playTrackAsByteBuffer() { return __vector_as_bytebuffer(6, 1); }
    public long msPlayed() { int o = __offset(8); return o != 0 ? bb.getLong(o + bb_pos) : 0L; }

    public static int createEndsong(FlatBufferBuilder builder,
                                    int user_idOffset,
                                    int play_trackOffset,
                                    long ms_played) {
        builder.startObject(3);
        Endsong.addMsPlayed(builder, ms_played);
        Endsong.addPlayTrack(builder, play_trackOffset);
        Endsong.addUserId(builder, user_idOffset);
        return Endsong.endEndsong(builder);
    }

    public static void startEndsong(FlatBufferBuilder builder) { builder.startObject(3); }
    public static void addUserId(FlatBufferBuilder builder, int userIdOffset) { builder.addOffset(0, userIdOffset, 0); }
    public static void addPlayTrack(FlatBufferBuilder builder, int playTrackOffset) { builder.addOffset(1, playTrackOffset, 0); }
    public static void addMsPlayed(FlatBufferBuilder builder, long msPlayed) { builder.addLong(2, msPlayed, 0L); }
    public static int endEndsong(FlatBufferBuilder builder) {
        int o = builder.endObject();
        return o;
    }
    public static void finishEndsongBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }
}
