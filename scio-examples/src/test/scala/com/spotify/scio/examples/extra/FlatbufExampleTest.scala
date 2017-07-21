package com.spotify.scio.examples.extra

import com.google.flatbuffers.FlatBufferBuilder
import com.spotify.scio.testing.{FlatbufIO, PipelineSpec}
import org.apache.beam.examples.Endsong

class FlatbufExampleTest extends PipelineSpec {

  def makeEndSong(userId: String, playTrack: String, msPlayed: Int): Endsong = {
    val builder = new FlatBufferBuilder(0)
    // Create Flatbuffer.
    val userIdIx = builder.createString(userId)
    val playTrackIx = builder.createString(playTrack)
    val es: Int = Endsong.createEndsong(builder, userIdIx, playTrackIx, msPlayed.toLong)
    builder.finish(es)
    Endsong.getRootAsEndsong(builder.dataBuffer())
  }

  val input = Seq(makeEndSong("user1", "track1", 1000),
                  makeEndSong("user2", "track1", 2000),
                  makeEndSong("user3", "track2", 3000))
  val expected = Seq(makeEndSong("user1test", "track1test", 1010),
    makeEndSong("user2test", "track1test", 2010),
    makeEndSong("user3test", "track2test", 3010))

  "FlatbufExampleTest" should "work" in {
    JobTest[com.spotify.scio.examples.extra.FlatbufExample.type]
      .args("--input=in.fb", "--output=out.fb")
      .input(FlatbufIO[Endsong]("in.fb"), input)
      .output[Endsong](FlatbufIO[Endsong]("out.fb"))(_ should containInAnyOrder[Endsong](expected))
      .run()
  }

}
