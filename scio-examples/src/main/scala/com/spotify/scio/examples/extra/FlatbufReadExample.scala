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

object FlatbufReadExample {

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.flatbufFile[Endsong](args("input"))
      .map((es: Endsong) => (es.userId(), es.playTrack(), es.msPlayed()))
      .saveAsTextFile("flatbufIOExample.txt")
    sc.close()

  }
}
