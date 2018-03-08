package com.spotify.scio.coders

import java.lang.reflect.Method
import java.nio.ByteBuffer

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.google.flatbuffers.Table
import com.twitter.chill.KSerializer

object FlatbufSerializer {
  val getterCache = scala.collection.mutable.Map[String,Method]()
}

private class FlatbufSerializer extends KSerializer[Table] {

  override def write(kryo: Kryo, out: Output, obj: Table): Unit = {
    val bb = obj.getByteBuffer
    out.writeInt(bb.array().length)
    out.write(bb.array())
  }

  override def read(kryo: Kryo, in: Input, cls: Class[Table]): Table = {
    // Get ByteBuffer
    val arrayLength = in.readInt()
    val bb = ByteBuffer.wrap(in.readBytes(arrayLength))
    // Fetch Flatbuf class that wraps this ByteBuffer
    val className = cls.getSimpleName
    // Retrieve class getter method. Cache for performance reasons.
    val bbToFlatbufGetter = FlatbufSerializer.getterCache.get(className) match {
      case Some(method) => method
      case None => {
        val getterMethod = cls.getMethod("getRootAs" + className, classOf[ByteBuffer])
        FlatbufSerializer.getterCache += (className -> getterMethod)
        getterMethod
      }
    }
    bbToFlatbufGetter.invoke(null, bb).asInstanceOf[Table]
  }
}
