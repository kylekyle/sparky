import java.io.InputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

import org.apache.commons.io.IOUtils;
import org.objectweb.asm.ClassReader;
import org.jruby.Ruby;
import org.jruby.embed.PathType;
import org.jruby.embed.ScriptingContainer;
import org.jruby.util.ClassDefiningJRubyClassLoader;
import scala.reflect.ClassManifestFactory$;
import org.apache.spark.serializer.JavaSerializer;
import org.apache.spark.serializer.SerializerInstance;


public final class Sparky {
    public static byte[] serialize(Serializable object) {
        SerializerInstance serializer = new JavaSerializer().newInstance();
        return serializer.serialize(object, ClassManifestFactory$.MODULE$.fromClass(Serializable.class)).array();
    }

    public static Object deserialize(byte[] klass, byte[] instance) throws Exception {
        Ruby.setThreadLocalRuntime(Ruby.getGlobalRuntime());

        ClassReader cr = new ClassReader(klass);
        String className = cr.getClassName().replace('/', '.');

        ClassDefiningJRubyClassLoader loader = new ClassDefiningJRubyClassLoader(Sparky.class.getClassLoader());
        loader.defineClass(className, klass);
        
        SerializerInstance serializer = new JavaSerializer().newInstance();
        return serializer.deserialize(ByteBuffer.wrap(instance), loader, ClassManifestFactory$.MODULE$.fromClass(Object.class));
    }

    public static void main(String[] args) throws Exception {
        ScriptingContainer engine = new ScriptingContainer();

        // this is the hack I came up with to get this jar to ignore system gems
        engine.runScriptlet("ENV.delete 'GEM_PATH'; ENV.delete 'GEM_HOME'");

        engine.runScriptlet(PathType.RELATIVE, args[0]);
    }
}