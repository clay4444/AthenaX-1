/*
 * AthenaX REST API
 * AthenaX REST API
 *
 * OpenAPI spec version: 0.1
 * Contact: haohui@uber.com
 *
 * NOTE: This class is auto generated by the swagger code generator program.
 * https://github.com/swagger-api/swagger-codegen.git
 * Do not edit the class manually.
 */


package com.uber.athenax.backend.api;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.uber.athenax.backend.api.JobDefinition;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.UUID;
import javax.validation.constraints.*;

/**
 * ExtendedJobDefinition
 */
@javax.annotation.Generated(value = "io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2018-03-06T14:54:29.251+05:30")
public class ExtendedJobDefinition   {
  @JsonProperty("uuid")
  private UUID uuid = null;

  @JsonProperty("definition")
  private JobDefinition definition = null;

  public ExtendedJobDefinition uuid(UUID uuid) {
    this.uuid = uuid;
    return this;
  }

  /**
   * the job uuid
   * @return uuid
   **/
  @JsonProperty("uuid")
  @ApiModelProperty(value = "the job uuid")
  public UUID getUuid() {
    return uuid;
  }

  public void setUuid(UUID uuid) {
    this.uuid = uuid;
  }

  public ExtendedJobDefinition definition(JobDefinition definition) {
    this.definition = definition;
    return this;
  }

  /**
   * Get definition
   * @return definition
   **/
  @JsonProperty("definition")
  @ApiModelProperty(value = "")
  public JobDefinition getDefinition() {
    return definition;
  }

  public void setDefinition(JobDefinition definition) {
    this.definition = definition;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ExtendedJobDefinition extendedJobDefinition = (ExtendedJobDefinition) o;
    return Objects.equals(this.uuid, extendedJobDefinition.uuid) &&
        Objects.equals(this.definition, extendedJobDefinition.definition);
  }

  @Override
  public int hashCode() {
    return Objects.hash(uuid, definition);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ExtendedJobDefinition {\n");
    
    sb.append("    uuid: ").append(toIndentedString(uuid)).append("\n");
    sb.append("    definition: ").append(toIndentedString(definition)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(java.lang.Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }
}
