package com.uber.athenax.backend.api;

import com.uber.athenax.backend.api.*;
import com.uber.athenax.backend.api.ClusterApiService;
import com.uber.athenax.backend.api.factories.ClusterApiServiceFactory;

import io.swagger.annotations.ApiParam;
import io.swagger.jaxrs.*;

import com.sun.jersey.multipart.FormDataParam;
import javax.validation.constraints.*;

import com.uber.athenax.backend.api.ClusterInfo;

import java.util.List;
import com.uber.athenax.backend.api.NotFoundException;

import java.io.InputStream;

import com.sun.jersey.core.header.FormDataContentDisposition;
import com.sun.jersey.multipart.FormDataParam;

import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.*;

@Path("/cluster")


@io.swagger.annotations.Api(description = "the cluster API")
@javax.annotation.Generated(value = "io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2018-03-06T14:54:29.251+05:30")
public class ClusterApi  {
   private final ClusterApiService delegate = ClusterApiServiceFactory.getClusterApi();

    @GET
    @Path("/info")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "Provide overall information of the cluster", notes = "", response = ClusterInfo.class, tags={ "cluster" })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "successful operation", response = ClusterInfo.class) })
    public Response getClusterInfo(
        @Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getClusterInfo(securityContext);
    }
}
