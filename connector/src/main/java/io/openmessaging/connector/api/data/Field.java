package io.openmessaging.connector.api.data;

public class Field {

  /** Index of a field. The index of fields in a schema should be unique and continuous。 */
  private int index;

  /** The name of a file. Should be unique in a shcema. */
  private String name;

  /** The type of the file. */
  private FieldType type;

  public Field(int index, String name, FieldType type) {

    this.index = index;
    this.name = name;
    this.type = type;
  }

  public int getIndex() {
    return index;
  }

  public void setIndex(int index) {
    this.index = index;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public FieldType getType() {
    return type;
  }

  public void setType(FieldType type) {
    this.type = type;
  }
}
