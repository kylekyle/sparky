import org.apache.spark.api.java.function.FlatMapFunction;
import org.jruby.Ruby;
import org.jruby.javasupport.JavaEmbedUtils;

public class SparkyFlatMapFunction implements FlatMapFunction {
    public byte[] klass;
    public byte[] instance;

    public Iterable call(Object o) throws Exception {
        Object rubyInstance = Sparky.deserialize(klass, instance);
        return (Iterable) JavaEmbedUtils.invokeMethod(Ruby.getGlobalRuntime(), rubyInstance, "call", new Object[]{o}, Iterable.class);
    }
}
