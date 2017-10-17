package org.dianahep

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

package object mlmpjr {
  val inputSchema = StructType(
    Nil 
    :+ StructField("EFlowTrack", ArrayType(StructType(
      Nil :+ StructField("PID", IntegerType) :+
      StructField("PT", FloatType) :+ StructField("Eta", FloatType) :+
      StructField("Phi", FloatType) :+ StructField("X", FloatType) :+ StructField("Y", FloatType) :+
      StructField("Z", FloatType)
    )))
    :+ StructField("EFlowNeutralHadron", ArrayType(StructType(
      Nil :+ StructField("ET", FloatType) :+ StructField("Eta", FloatType) :+
      StructField("Phi", FloatType)
    )))
    :+ StructField("EFlowPhoton", ArrayType(StructType(
      Nil :+ StructField("ET", FloatType) :+ StructField("Eta", FloatType) :+
      StructField("Phi", FloatType)
    )))
    :+ StructField("Electron", ArrayType(StructType(
      Nil :+ StructField("PT", FloatType) :+ StructField("Eta", FloatType) :+
      StructField("Phi", FloatType) :+ StructField("Charge", IntegerType)
    )))
    :+ StructField("MuonTight", ArrayType(StructType(
      Nil :+ StructField("PT", FloatType) :+ StructField("Eta", FloatType) :+
      StructField("Phi", FloatType) :+ StructField("Charge", IntegerType)
    )))
    :+ StructField("MissingET", ArrayType(StructType(
      Nil :+ StructField("MET", FloatType) :+ StructField("Phi", FloatType)
    )))
    :+ StructField("Jet", ArrayType(StructType(
      Nil :+ StructField("PT", FloatType) :+ StructField("Eta", FloatType) :+
      StructField("BTag", IntegerType)
    )))
  )

  case class PtEtaPhi(PT: Float, Eta: Float, Phi: Float)
  case class PtEtaPhiCharge(PT: Float, Eta: Float, Phi: Float, Charge: Int)
  case class PIDPtEtaPhiXYZ(PID: Int, Pt: Float, Eta: Float, Phi: Float, X: Float, Y: Float, Z: Float)
  case class EtEtaPhi(ET: Float, Eta: Float, Phi: Float)
  case class METPhi(MET: Float, Phi: Float)
  case class PtEtaBTag(Pt: Float, Eta: Float, BTag: Float)
  case class EventIn(
      tracks: Seq[PIDPtEtaPhiXYZ], 
      neutralHadrons: Seq[EtEtaPhi],
      photons: Seq[EtEtaPhi],
      electrons: Seq[PtEtaPhiCharge],
      muons: Seq[PtEtaPhiCharge],
      met: Seq[METPhi],
      jets: Seq[PtEtaBTag]
      )
  case class ParticleContent(
      E: Float, Px: Float, Py: Float, Pz: Float,
      Pt: Float, Eta: Float, Phi: Float, 
      X: Float, Y: Float, Z: Float,
      isoCh: Float, isoGamma: Float, isoNeutral: Float, 
      n1: Int, n2: Int, n3: Int, n4: Int, n5: Int, charge: Float)
  case class EventOut(
      lepton: ParticleContent,
      tracks: Seq[ParticleContent],
      photons: Seq[ParticleContent],
      neutrals: Seq[ParticleContent]
      )

  def toEventIn(df: Dataset[Row]): Dataset[EventIn] = 
    df.toDF("tracks", "neutralHadrons", "photons", "electrons", "muons", "met", "jets").as[EventIn]

  def toEventOut(ds: Dataset[EventIn]): Dataset[EventOut] =
    ds.map({ case e =>
      EventOut()
      })

  /*
  def slim(events: Dataset[Event]): Dataset[EventEnriched] = 
    events map {case e => EventEnriched(e.tracks, e.neutralHadrons, e.photons, e.electrons, e.muons, e.met, e.jets,
      e.tracks.filter(_.PT>0.5), e.photons.filter())}
  */

  def filterStep(ds: Dataset[EventIn]): Dataset[EventIn] = 
    // slimd down the main objects
    ds.map({
        case e => Event(e.tracks.filter(_.PT>0.5), e.neutralHadrons.filter(_.ET>1.0), e.photons.filter(_.ET>1.0), 
          e.electrons, e.muons, e.met, e.jets)})
    // filter out events where at least 1 of the tracks/photons/neturals is not present at all
      .filter({
        e: Event => e.tracks.size>0 && e.neutralHadrons.size>0 && e.photons.size>0})
      .filter({
        e: Event => e.electrons.size>0 || e.muons.size>0})

  // Select leptons
  def selectLeptons(events: Dataset[EventIn]): Dataset[EventIn] = 
    events.map({
      case e => Event(e.tracks, e.neutralHadrons, e.photons, 
        e.electrons.filter(_.PT>25),
        e.muons.filter(_.PT>25)
      )})
}
