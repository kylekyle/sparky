package org.apache.spark.api.ruby;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.jruby.embed.PathType;
import org.jruby.embed.ScriptingContainer;
import org.jruby.RubyInstanceConfig;
import org.jruby.RubyProc;
import org.jruby.RubyString;

public class Sparky implements FlatMapFunction, Function, Function2, Serializable {

  private final byte[] bytes;
  private transient RubyProc proc = null;
  private transient ScriptingContainer engine = newScriptingContainer();

  public Sparky(RubyProc proc) {
    this.proc = proc;
    this.bytes = engine.callMethod(proc, "to_bytes", RubyString.class).getBytes();
  }

  public RubyProc getProc() {
    if (engine == null) {
      engine = newScriptingContainer();
    }

    if (proc == null) {
      engine.put("bytes", bytes);
      proc = (RubyProc) engine.runScriptlet("Proc.from_bytes(String.from_java_bytes(bytes))");
    }
    
    return proc;
  }

  public static void main(String[] args) throws Exception {
    ScriptingContainer engine = newScriptingContainer();

    if (args.length == 0) {
      engine.runScriptlet("require 'sparky/shell'");
    } else {
      engine.put("ARGV", Arrays.copyOfRange(args, 1, args.length));
      engine.runScriptlet(PathType.RELATIVE, args[0]);
    }
  }

  @Override
  public Iterable call(Object o) throws Exception {
    getProc();
    return engine.callMethod(getProc(), "call", o, Iterable.class);
  }

  @Override
  public Object call(Object o1, Object o2) throws Exception {
    getProc();
    return engine.callMethod(getProc(), "call", new Object[]{o1, o2});
  }

  private static ScriptingContainer newScriptingContainer() {
    return new ScriptingContainer() {
      {
        setCompileMode(RubyInstanceConfig.CompileMode.OFF);
        runScriptlet("ENV.delete 'GEM_PATH'; ENV.delete 'GEM_HOME'");
        runScriptlet("require 'org/apache/spark/api/ruby/proc_to_bytes'");
      }
    };
  }
}
