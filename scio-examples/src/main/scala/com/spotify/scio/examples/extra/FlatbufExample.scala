/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.examples.extra

import com.google.flatbuffers.FlatBufferBuilder
import com.spotify.scio.ContextAndArgs
import org.apache.beam.examples.Endsong

object FlatbufExample {

  def makeEndSong(userId: String, playTrack: String, msPlayed: Int): Endsong = {
    val builder = new FlatBufferBuilder(0)
    // Create Flatbuffer.
    val userIdIx = builder.createString(userId)
    val playTrackIx = builder.createString(playTrack)
    val es: Int = Endsong.createEndsong(builder, userIdIx, playTrackIx, msPlayed.toLong)
    builder.finish(es)
    Endsong.getRootAsEndsong(builder.dataBuffer())
  }

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    sc.flatbufFile[Endsong](args("input"))
      .map((fb: Endsong) => makeEndSong(fb.userId() + "test",
                                        fb.playTrack() + "test",
                                        fb.msPlayed().toInt + 10))
      .saveAsFlatbufFile(args("output"))

    sc.close()
  }
}
