package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    //    private int numFields;
    private List<TDItem> fields;

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return null;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        fields = new ArrayList<>();
        TDItem item = null;
        for (int i = 0; i < typeAr.length; i++) {
            if (i < fieldAr.length) {
                item = new TDItem(typeAr[i], fieldAr[i]);
            } else {
                item = new TDItem(typeAr[i], null);
            }
            fields.add(item);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        fields = new ArrayList<>();
        TDItem item = null;
        for (Type type : typeAr) {
            item = new TDItem(type, null);
            fields.add(item);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return fields.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        return fields.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        return fields.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        return IntStream.range(0, numFields())
                .filter(i -> fields.get(i).fieldName != null && fields.get(i).fieldName.equals(name))
                .findAny()
                .orElseThrow(NoSuchElementException::new);
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        AtomicInteger sumSize = new AtomicInteger(0);
        fields.forEach(tdItem -> {
            if (tdItem.fieldType == Type.INT_TYPE) {
                sumSize.addAndGet(Type.INT_TYPE.getLen());
            }
            if (tdItem.fieldType == Type.STRING_TYPE) {
                sumSize.addAndGet(Type.STRING_TYPE.getLen());
            }
        });
        return sumSize.get();
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        Type[] types = new Type[td1.numFields() + td2.numFields()];
        String[] fieldNames = new String[td1.numFields() + td2.numFields()];
        int index = 0;
        for (TDItem field : td1.fields) {
            types[index] = field.fieldType;
            fieldNames[index] = field.fieldName;
            index += 1;
        }
        for (TDItem field : td2.fields) {
            types[index] = field.fieldType;
            fieldNames[index] = field.fieldName;
            index += 1;
        }
        return new TupleDesc(types, fieldNames);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if (o == null) {
            return false;
        }
        if (!(o instanceof TupleDesc)) {
            return false;
        }
        TupleDesc ot = (TupleDesc) o;
        if (numFields() != ot.numFields()) {
            return false;
        }
        for (int i = 0; i < numFields(); i++) {
            TDItem td01 = fields.get(i);
            TDItem td02 = ot.fields.get(i);
            if (td01.fieldType != td02.fieldType) {
                return false;
            }
            if (td01.fieldName == null && td02.fieldName != null) {
                return false;
            }
            if (td01.fieldName != null && td02.fieldName == null) {
                return false;
            }
            if (td01.fieldName != null && !td01.fieldName.equals(td02.fieldName)) {
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        return "";
    }
}
