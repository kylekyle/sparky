package org.apache.spark.api.ruby;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map.Entry;
import org.jruby.RubyInstanceConfig;
import org.jruby.ir.IRClosure;
import org.jruby.ir.IRScope;
import org.jruby.ir.IRScopeType;
import org.jruby.ir.Operation;
import org.jruby.ir.instructions.Instr;
import org.jruby.ir.instructions.specialized.ZeroOperandArgNoBlockCallInstr;
import org.jruby.ir.interpreter.InterpreterContext;
import org.jruby.ir.operands.LocalVariable;
import org.jruby.ir.operands.Operand;
import org.jruby.ir.operands.OperandType;
import org.jruby.ir.persistence.IRWriterStream;
import org.jruby.parser.StaticScope;
import org.jruby.runtime.RubyEvent;
import org.jruby.runtime.Signature;

/**
 *
 * @author kyle
 */
class ProcWriter extends IRWriterStream {

  private final ObjectOutputStream output;

  public ProcWriter(ObjectOutputStream output) {
    super(output);
    this.output = output;
  }

  @Override
  public void encode(String value) {
    try {
      output.writeUTF(value);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void encode(String[] values) {
    encode(values.length);
    for (String value : values) {
      encode(value);
    }
  }

  @Override
  public void encode(Instr value) {
    encode(value.getOperation());
    encode(value.getOperands());

  }

  @Override
  public void encode(IRScope scope) {
    encode(scope.getLineNumber());

    if (!(scope instanceof IRClosure)) {
      throw new UnsupportedOperationException("Expected an IRClosure, got an " + scope.getClass().getName());
    }

    IRClosure closure = (IRClosure) scope;
    encode(closure.getSignature());

    InterpreterContext context = closure.getInterpreterContext();
    encode(context.getInstructions().length);

    int index = 1;
    for (Instr i : context.getInstructions()) {
      if (RubyInstanceConfig.IR_WRITING_DEBUG) {
        System.err.println("Encode " + index++ + ": " + i.getClass().getName());
      }

      if (i instanceof ZeroOperandArgNoBlockCallInstr) {
        System.out.println(((ZeroOperandArgNoBlockCallInstr) i).getClosureArg());
      }

      i.encode(this);
    }

    encode(closure.getLocalVariablesCount());

    if (closure.getLocalVariablesCount() > 0) {
      for (Entry<String, LocalVariable> entry : closure.getLocalVariables().entrySet()) {
        encode(entry.getKey());
        encode(entry.getValue());
      }
    }
  }

  @Override
  public void encode(IRScopeType value) {
    try {
      output.writeObject(value);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void encode(Signature signature) {
    encode(signature.encode());
  }

  @Override
  public void encode(RubyEvent event) {
    try {
      output.writeObject(event);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void encode(StaticScope.Type value) {
    try {
      output.writeObject(value);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void encode(Operation value) {
    encode(value.ordinal());
  }

  @Override
  public void encode(Operand value) {
    value.encode(this);
  }

  @Override
  public void encode(Operand[] value) {
    encode(value.length);
    for (Operand operand : value) {
      encode(operand);
    }
  }

  @Override
  public void encode(OperandType value) {
    encode(value.getCoded());
  }

  @Override
  public void encode(byte[] values) {
    encode(values.length);
    for (byte value : values) {
      encode(value);
    }
  }

  @Override
  public void encode(boolean value) {
    try {
      output.writeBoolean(value);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void encode(byte value) {
    try {
      output.writeByte(value);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void encode(char value) {
    try {
      output.writeChar(value);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void encode(int value) {
    try {
      output.writeInt(value);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void encode(long value) {
    try {
      output.writeLong(value);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void encode(double value) {
    try {
      output.writeDouble(value);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void encode(float value) {
    try {
      output.writeFloat(value);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void startEncodingScopeHeader(IRScope scope) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void endEncodingScopeHeader(IRScope scope) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void startEncodingScopeInstrs(IRScope scope) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void endEncodingScopeInstrs(IRScope scope) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void startEncodingScopeHeaders(IRScope script) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void endEncodingScopeHeaders(IRScope script) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void startEncoding(IRScope scope) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void endEncoding(IRScope script) {
    throw new UnsupportedOperationException();
  }

}
