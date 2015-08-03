package org.apache.spark.api.ruby;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.jruby.Ruby;
import org.jruby.RubyInstanceConfig;
import org.jruby.RubyProc;
import org.jruby.RubyString;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.ir.IRClosure;
import org.jruby.ir.operands.WrappedIRClosure;
import org.jruby.runtime.Block;
import org.jruby.runtime.InterpretedIRBlockBody;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.runtime.load.BasicLibraryService;
import org.jruby.util.ByteList;

@JRubyClass(name = "ProcToBytes")
public class ProcToBytes implements BasicLibraryService {

  @JRubyMethod
  public static IRubyObject to_bytes(ThreadContext context, IRubyObject self) {
    Ruby ruby = context.getRuntime();
    RubyProc proc = (RubyProc) self;
    Block block = proc.getBlock();

    if (ruby.getInstanceConfig().getCompileMode() != RubyInstanceConfig.CompileMode.OFF) {
      throw ruby.newRuntimeError("Only interpreted procs can be serialized. Use the -X-C flag or set jruby.compile.mode=OFF .");
    }

    if (!(block.getBody() instanceof InterpretedIRBlockBody)) {
      throw ruby.newRuntimeError("Cannot serialize " + block.getBody().getClass().getName());
    }

    InterpretedIRBlockBody body = (InterpretedIRBlockBody) block.getBody();

    if (!(body.getIRScope() instanceof IRClosure)) {
      throw ruby.newRuntimeError("Our scope needs to be a closure");
    }

    IRClosure closure = (IRClosure) body.getIRScope();

    try {
      try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
        try (ObjectOutputStream output = new ObjectOutputStream(baos)) {
          new WrappedIRClosure(closure.getSelf(), closure).encode(new ProcWriter(output));
        }

        return ruby.newString(new ByteList(baos.toByteArray()));
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @JRubyMethod
  public static IRubyObject from_bytes(ThreadContext context, IRubyObject self, IRubyObject arg) throws IOException, ClassNotFoundException {
    byte[] bytes = ((RubyString) arg).getBytes();

    try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes)) {
      try (ObjectInputStream input = new ObjectInputStream(bais)) {
        ProcReader reader = new ProcReader(context, input);
        return context.getRuntime().newProc(Block.Type.PROC, reader.getBlock());
      }
    }
  }

  @Override
  public boolean basicLoad(Ruby runtime) throws IOException {
    runtime.getClass("Proc").defineAnnotatedMethod(ProcToBytes.class, "to_bytes");
    runtime.getClass("Proc").getMetaClass().defineAnnotatedMethod(ProcToBytes.class, "from_bytes");
    return true;
  }
}
