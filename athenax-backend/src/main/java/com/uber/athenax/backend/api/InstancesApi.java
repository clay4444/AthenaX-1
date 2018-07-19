package com.uber.athenax.backend.api;

import com.uber.athenax.backend.api.*;
import com.uber.athenax.backend.api.InstancesApiService;
import com.uber.athenax.backend.api.factories.InstancesApiServiceFactory;

import io.swagger.annotations.ApiParam;
import io.swagger.jaxrs.*;

import com.sun.jersey.multipart.FormDataParam;
import javax.validation.constraints.*;

import com.uber.athenax.backend.api.InstanceState;
import com.uber.athenax.backend.api.InstanceStatus;
import java.util.UUID;

import java.util.List;
import com.uber.athenax.backend.api.NotFoundException;

import java.io.InputStream;

import com.sun.jersey.core.header.FormDataContentDisposition;
import com.sun.jersey.multipart.FormDataParam;

import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.*;

@Path("/instances")


@io.swagger.annotations.Api(description = "the instances API")
@javax.annotation.Generated(value = "io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2018-03-06T14:54:29.251+05:30")
public class InstancesApi  {
   private final InstancesApiService delegate = InstancesApiServiceFactory.getInstancesApi();

    @PUT
    @Path("/{instanceUUID}/state")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "Change the state of the instance", notes = "", response = InstanceState.class, tags={ "apps",  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "successful operation", response = InstanceState.class) })
    public Response changeInstanceState(
        @ApiParam(value = "UUID of the instance",required=true) @PathParam("instanceUUID") UUID instanceUUID,
        @ApiParam(value = "The desired state of the instance" ,required=true) InstanceState state,
        @Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.changeInstanceState(instanceUUID,state,securityContext);
    }
    @GET
    @Path("/{instanceUUID}")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "Find the instance by UUID", notes = "", response = InstanceStatus.class, tags={ "apps",  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "successful operation", response = InstanceStatus.class) })
    public Response getInstanceInfo(
        @ApiParam(value = "UUID of the instance",required=true) @PathParam("instanceUUID") UUID instanceUUID,
        @Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getInstanceInfo(instanceUUID,securityContext);
    }
    @GET
    @Path("/{instanceUUID}/state")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "Get the state of the instance", notes = "", response = InstanceState.class, tags={ "apps" })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "successful operation", response = InstanceState.class) })
    public Response getInstanceState(
        @ApiParam(value = "UUID of the instance",required=true) @PathParam("instanceUUID") UUID instanceUUID,
        @Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getInstanceState(instanceUUID,securityContext);
    }
}
