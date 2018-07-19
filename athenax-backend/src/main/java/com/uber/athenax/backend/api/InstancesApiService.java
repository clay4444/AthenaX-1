package com.uber.athenax.backend.api;

import com.uber.athenax.backend.api.*;
import com.uber.athenax.backend.api.*;

import com.sun.jersey.multipart.FormDataParam;

import com.uber.athenax.backend.api.InstanceState;
import com.uber.athenax.backend.api.InstanceStatus;
import java.util.UUID;

import java.util.List;
import com.uber.athenax.backend.api.NotFoundException;

import java.io.InputStream;

import com.sun.jersey.core.header.FormDataContentDisposition;
import com.sun.jersey.multipart.FormDataParam;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.validation.constraints.*;
@javax.annotation.Generated(value = "io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2018-03-06T14:54:29.251+05:30")
public abstract class InstancesApiService {
      public abstract Response changeInstanceState(UUID instanceUUID,InstanceState state,SecurityContext securityContext)
      throws NotFoundException;
      public abstract Response getInstanceInfo(UUID instanceUUID,SecurityContext securityContext)
      throws NotFoundException;
      public abstract Response getInstanceState(UUID instanceUUID,SecurityContext securityContext)
      throws NotFoundException;
}
