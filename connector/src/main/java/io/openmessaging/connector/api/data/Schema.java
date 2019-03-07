package io.openmessaging.connector.api.data;

import java.util.List;

public class Schema {

  private String dataSource;
  private String name;
  private List<Field> fields;

  public String getDataSource() {
    return dataSource;
  }

  public void setDataSource(String dataSource) {
    this.dataSource = dataSource;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<Field> getFields() {
    return fields;
  }

  public void setFields(List<Field> fields) {
    this.fields = fields;
  }

  public Field getField(String fieldName) {

    for (Field field : fields) {
      if (field.getName().equals(fieldName)) {
        return field;
      }
    }
    return null;
  }
}
