package org.nts.pigutils.proto;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.BitSet;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;


@SuppressWarnings("serial")
/**
 * This is base class for Tuple implementations that delay parsing until
 * individual fields are requested.
 */
public abstract class AbstractLazyTuple implements Tuple {

  private static TupleFactory tf  = TupleFactory.getInstance();

  protected Tuple realTuple;
  protected boolean isRef; // i.e. reference() is invoked.
  protected BitSet idxBits;

  boolean isNull = false;
  
  protected void initRealTuple(int tupleSize) {
    realTuple = tf.newTuple(tupleSize);
    idxBits = new BitSet(tupleSize);
    isRef = false;
  }

  /**
   * Returns object for the given index. This is invoked only
   * once for each instance.
   */
  protected abstract Object getObjectAt(int index);

  public void append(Object obj) {
    realTuple.append(obj);
  }

  public Object get(int idx) throws ExecException {
    if (!isRef && !idxBits.get(idx)) {
      realTuple.set(idx, getObjectAt(idx));
      idxBits.set(idx);
    }
    return realTuple.get(idx);
  }

  public List<Object> getAll() {
    convertAll();
    return realTuple.getAll();
  }

  public long getMemorySize() {
    return realTuple.getMemorySize();
  }

  public byte getType(int idx) throws ExecException {
    get(idx);
    return realTuple.getType(idx);
  }

  public boolean isNull() {
    return isNull;
  }

  public boolean isNull(int idx) throws ExecException {
    get(idx);
    return realTuple.isNull(idx);
  }

  public void reference(Tuple t) {
    if (t != this) {
      realTuple = t;
      isRef = true; // don't invoke getObjetAt() anymore.
    }
  }

  public void set(int idx, Object val) throws ExecException {
    realTuple.set(idx, val);
    idxBits.set(idx);
  }

  public void setNull(boolean isNull) {
    this.isNull = isNull;
  }

  public int size() {
    return realTuple.size();
  }

  public String toDelimitedString(String delim) throws ExecException {
    convertAll();
    return realTuple.toDelimitedString(delim);
  }

  public void readFields(DataInput in) throws IOException {
    Tuple t = tf.newTuple(realTuple.size());
    t.readFields(in);
    reference(t);
  }

  public void write(DataOutput out) throws IOException {
    convertAll();
    realTuple.write(out);
  }

  @SuppressWarnings("unchecked")
  public int compareTo(Object arg0) {
    convertAll();
    return realTuple.compareTo(arg0);
  }

  protected void convertAll() {
    if (isRef) {
      return;
    }
    int size = realTuple.size();
    for (int i = 0; i < size; i++) {
      try {
        get(i);
      } catch (ExecException e) {
        throw new RuntimeException("Unable to process field " + i, e);
      }
    }
  }
}
