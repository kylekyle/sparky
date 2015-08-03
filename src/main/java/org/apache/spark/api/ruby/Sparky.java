package org.apache.spark.api.ruby;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import org.objectweb.asm.ClassReader;
import org.jruby.Ruby;
import org.jruby.embed.PathType;
import org.jruby.embed.ScriptingContainer;
import org.jruby.util.ClassDefiningJRubyClassLoader;
import scala.reflect.ClassManifestFactory$;
import org.apache.spark.serializer.JavaSerializer;
import org.apache.spark.serializer.SerializerInstance;
import org.jruby.javasupport.Java;
import org.jruby.javasupport.JavaEmbedUtils;

public class Sparky implements FlatMapFunction, Function, Function2 {

  private byte[] klass;
  private byte[] serialized;
  transient Object instance;

  public Sparky() {
  }

  public Sparky(byte[] klass, Serializable object) {
    this.klass = klass;

    SerializerInstance serializer = new JavaSerializer().newInstance();
    serialized = serializer.serialize(object, ClassManifestFactory$.MODULE$.fromClass(Serializable.class)).array();
  }

  private Object instance() throws Exception {
    if (instance == null) {
      if (Ruby.getThreadLocalRuntime() == null) {
        Ruby.setThreadLocalRuntime(Ruby.newInstance());
      }

      ClassReader cr = new ClassReader(klass);
      String className = cr.getClassName().replace('/', '.');

      ClassDefiningJRubyClassLoader loader = Ruby.getThreadLocalRuntime().getJRubyClassLoader();
      loader.defineClass(className, klass);
      //.evalScriptlet("import 'rubyobj.AnonymousRubyClass__1356'");
      SerializerInstance serializer = new JavaSerializer().newInstance();
      instance = serializer.deserialize(ByteBuffer.wrap(serialized), loader, ClassManifestFactory$.MODULE$.fromClass(Object.class));
    }

    return instance;
  }

  public static void main(String[] args) throws Exception {
    ScriptingContainer engine = new ScriptingContainer();

    // this is the hack I came up with to get this jar to ignore system gems
    engine.runScriptlet("ENV.delete 'GEM_PATH'; ENV.delete 'GEM_HOME'");

    if (args.length == 0) {
      engine.runScriptlet("require 'sparky/shell'");
    } else {
      engine.put("ARGV", Arrays.copyOfRange(args, 1, args.length));
      engine.runScriptlet(PathType.RELATIVE, args[0]);
    }
  }

  public Iterable call(Object t) throws Exception {
    return (Iterable) JavaEmbedUtils.invokeMethod(Ruby.getGlobalRuntime(), instance(), "call", new Object[]{t}, Iterable.class);
  }

  public Object call(Object v1, Object v2) throws Exception {
    return JavaEmbedUtils.invokeMethod(Ruby.getGlobalRuntime(), instance(), "call", new Object[]{v1, v2}, Object.class);
  }
}
