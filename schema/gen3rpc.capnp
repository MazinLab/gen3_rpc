@0xba912cc2e31f4f86;

# Indicate that a method should have an exclusive self interface reference
# or a parameter should be an exclusive reference to the given interface type
# rust equivalent &mut self, or &mut blah for method and parameter respectively
annotation mut(method, param) :Void;

struct Rational {
  numerator @0 :Int64;
  denominator @1 :Int64;
}

struct ComplexInt16 {
  real @0 :Int16;
  imag @1 :Int16;
}

struct ComplexInt32 {
  real @0 :Int32;
  imag @1 :Int32;
}

# Do to limitations in the capnp encoding these need to be pointer types
interface Option(T) {
  struct Option {
    union {
      some @0: T;
      none @1: Void;
    }
  }

  get @0 () -> (option: Option);

  isSome @1 () -> (some: Bool);
  unwrap @2 () -> (some: T);
  unwrapOr @3 (or: T) -> (result: T);
}

# Do to limitations in the capnp encoding these need to be pointer types
interface Result(T, E) {
  struct Result {
    union {
      ok @0: T;
      error @1: E;
    }
  }
  get @0 () -> (result: Result);

  isOk @1 () -> (some: Bool);
  unwrap @2 () -> (some: T);
  unwrapOr @3 (or: T) -> (result: T);
}

struct Hertz {
  frequency @0: Rational;
}

interface DroppableReference {
  drop @0 ();
  isMut @1 () -> (mutable: Bool);
  dropMut @2 () -> (nonmut: DroppableReference) $mut;
  tryIntoMut @3() -> (maybeMut: Result(DroppableReference, DroppableReference));
}

interface IfBoard extends(DroppableReference) {
  struct Attens {
    input @0: Float32;
    output @1: Float32;
  }

  struct FreqError {
    union {
      couldntLock @0: Void;
      unachievable @1: Void;
    }
  }

  struct AttenError {
    union {
      unachievable @0: Void;
      unsafe @1: Void;
    }
  }

  getFreq @0 () -> (freq: Hertz);
  setFreq @1 (freq: Hertz) -> (freq: Result(Hertz, FreqError)) $mut;
  getAttens @2 () -> (attens: Attens);
  setAttens @3 (attens: Attens) -> (attens: Result(Attens, AttenError)) $mut;
}

interface DspScale extends(DroppableReference) {
  struct Scale16 {
    scale @0: UInt16;
  }
  getFftScale @0 () -> (scale: Scale16);
  setFftScale @1 (scale: Scale16) -> (scale: Result(Scale16, Scale16));
}

interface DdcChannel extends(DroppableReference) {
  struct ChannelConfig {
    sourceBin @0 :UInt32;
    ddcFreq @1: Int32;
    destinationBin: union {
      none @2: Void;
      some @3: UInt32;
    }
    rotation @4: Int32 = 0;
    center @5: ComplexInt32 = (real = 0, imag = 0);
  }

  get @0 () -> ChannelConfig;
  set @1 (replace: ChannelConfig) $mut;

  setSource @2 (sourceBin :UInt16) $mut;
  setDdcFreq @3 (ddcFreq :Int16) $mut;
  setRotation @4 (rotation :Int16) $mut;
  setCenter @5 (center :ComplexInt16) $mut;

  getBasebandFrequency @6 () -> (frequency :Hertz);
}

interface Ddc {
  struct ChannelAllocationError {
    union {
      outOfChannels @0: Void;
      destinationInUse @1: Void;
      sourceDestIncompatible @2: Void;
    }
  }

  struct Capabilities {
    freqResolution @0 :Rational;
    freqBits @1 :UInt16;
    rotationBits @2: UInt16;
    centerBits @3: UInt16;

    binControl: union {
      fullSwizzle @4: Void;
      none @5: Void;
    }
  }

  capabilities @0 () -> Capabilities;

  allocateChannel @1 (config: DdcChannel.ChannelConfig) -> (result :Result(DdcChannel, ChannelAllocationError));
  allocateChannelMut @2 (config: DdcChannel.ChannelConfig) -> (result :Result(DdcChannel, ChannelAllocationError));

  retrieveChannel @3 (config: DdcChannel.ChannelConfig) -> (channel :Option(DdcChannel));
}


interface DacTable extends(DroppableReference) {
  struct DacTable {
    data @0 :List(ComplexInt16);
  }

  get @0 () -> DacTable;
  set @1 (replace: DacTable) $mut;
}

interface Snap extends(DroppableReference) {
  struct Snap {
    union {
      rawIq @0: List(ComplexInt16);
      ddcIq @1: List(List(ComplexInt16));
      phase @2: List(List(Int16));
    }
  }

  get @0 () -> Snap;
}

interface Capture {
  struct CaptureError {
    union {
      unsupportedTap @0: Void;
      memoryUnavilable @1: Void;
    }
  }

  struct CaptureTap {
    union {
      rawIq @0: Void;
      ddcIq @1: List(DdcChannel);
      phase @2: List(DdcChannel);
    }
  }

  capture @0 (tap: CaptureTap, length: UInt64) -> (result: Result(Snap, CaptureError));
}

interface Gen3Board {
  getDdc @0 () -> (ddc: Ddc);
  getDacTable @1 () -> (dacTable: DacTable);
  getCapture @2 () -> (capture: Capture);
  getDspScale @3 () -> (dspScale: DspScale);
  getIfBoard @4 () -> (ifBoard: IfBoard);
}
