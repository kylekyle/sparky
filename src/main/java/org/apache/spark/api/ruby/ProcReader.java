package org.apache.spark.api.ruby;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.jruby.RubyInstanceConfig;
import org.jruby.ir.IRClosure;
import org.jruby.ir.IRScope;
import org.jruby.ir.IRScopeType;
import org.jruby.ir.instructions.Instr;
import org.jruby.ir.instructions.specialized.ZeroOperandArgNoBlockCallInstr;
import org.jruby.ir.interpreter.InterpreterContext;
import org.jruby.ir.operands.Label;
import org.jruby.ir.operands.LocalVariable;
import org.jruby.ir.operands.Operand;
import org.jruby.ir.operands.TemporaryVariableType;
import org.jruby.ir.operands.WrappedIRClosure;
import org.jruby.parser.StaticScope;
import org.jruby.runtime.RubyEvent;
import org.jruby.runtime.Signature;
import org.jruby.ir.operands.OperandType;
import org.jruby.ir.persistence.IRReaderStream;
import org.jruby.runtime.Block;
import org.jruby.runtime.DynamicScope;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.scope.ManyVarsDynamicScope;
import org.jruby.util.ByteList;

/**
 *
 * @author kyle
 */
class ProcReader extends IRReaderStream {

  private IRScope currentScope;
  private final ThreadContext context;
  private final ObjectInputStream input;

  public ProcReader(ThreadContext context, ObjectInputStream input) {
    super(context.getRuntime().getIRManager(), new ByteArrayInputStream(new byte[]{}), new ByteList("foo.rb".getBytes()));
    this.input = input;
    this.context = context;
    this.currentScope = context.getCurrentStaticScope().getIRScope();
  }

  @Override
  public String decodeString() {
    try {
      return input.readUTF();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public String[] decodeStringArray() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public IRScopeType decodeIRScopeType() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public StaticScope.Type decodeStaticScopeType() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public List<Operand> decodeOperandList() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public OperandType decodeOperandType() {
    return OperandType.fromCoded(decodeByte());
  }

  @Override
  public boolean decodeBoolean() {
    try {
      return input.readBoolean();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public byte decodeByte() {
    try {
      return input.readByte();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public byte[] decodeByteArray() {
    int numBytes = decodeInt();
    byte[] byteArray = new byte[numBytes];

    for (int i = 0; i < numBytes; i++) {
      byteArray[i] = decodeByte();
    }

    return byteArray;
  }

  @Override
  public char decodeChar() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public int decodeInt() {
    try {
      return input.readInt();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public int decodeIntRaw() {
    return decodeInt();
  }

  @Override
  public long decodeLong() {
    try {
      return input.readLong();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public double decodeDouble() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public float decodeFloat() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Label decodeLabel() {
    return (Label) decodeOperand();
  }

  @Override
  public RubyEvent decodeRubyEvent() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public List<Instr> decodeInstructionsAt(IRScope scope, int offset) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public IRScope getCurrentScope() {
    return currentScope;
  }

  @Override
  public Map<String, Operand> getVars() {
    return new HashMap();
  }

  @Override
  public void addScope(IRScope scope) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void seek(int headersOffset) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public IRScope decodeScope() {
    int line = decodeInt();
    Signature signature = decodeSignature();
    int numInstructions = decodeInt();

    Instr[] instructions = new Instr[numInstructions];

    for (int i = 0; i < numInstructions; i++) {
      if (RubyInstanceConfig.IR_WRITING_DEBUG) {
        System.err.print("Decode " + (i + 1) + ": ");
      }

      instructions[i] = decodeInstr();

      if (RubyInstanceConfig.IR_WRITING_DEBUG) {
        System.err.println(instructions[i].getClass().getName());
      }

      if (instructions[i] instanceof ZeroOperandArgNoBlockCallInstr) {
        System.out.println(((ZeroOperandArgNoBlockCallInstr) instructions[i]).getClosureArg());
      }
    }

    IRScope lexicalParent = new IRScope(context.getRuntime().getIRManager(), currentScope, "(serialized proc)", "(serialized proc)", 0, null) {

      @Override
      public IRScopeType getScopeType() {
        return IRScopeType.CLOSURE;
      }
    };

    StaticScope staticScope = lexicalParent.getTopLevelScope().getStaticScope();
    IRClosure closure = new IRClosure(context.getRuntime().getIRManager(), lexicalParent, line, staticScope, signature);

    InterpreterContext ic = new InterpreterContext(currentScope, Arrays.asList(instructions));
    closure.setInterpreterContext(ic);

    int numLocalVariables = decodeInt();

    if (numLocalVariables > 0) {
      Map<String, LocalVariable> variables = new HashMap<>();
      variables.put(decodeString(), (LocalVariable) decodeOperand());
      closure.setLocalVariables(variables);
    }

    return closure;
  }

  @Override
  public TemporaryVariableType decodeTemporaryVariableType() {
    return TemporaryVariableType.fromOrdinal(decodeByte());
  }

  Block getBlock() {
    WrappedIRClosure closure = (WrappedIRClosure) decodeOperand();
    StaticScope staticScope = context.getCurrentStaticScope();
    DynamicScope dynamicScope = new ManyVarsDynamicScope(staticScope);
    return (Block) closure.retrieve(context, context.getFrameSelf(), staticScope, dynamicScope, null);
  }

}
