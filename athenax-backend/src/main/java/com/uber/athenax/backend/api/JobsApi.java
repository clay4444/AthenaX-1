package com.uber.athenax.backend.api;

import com.uber.athenax.backend.api.*;
import com.uber.athenax.backend.api.JobsApiService;
import com.uber.athenax.backend.api.factories.JobsApiServiceFactory;

import io.swagger.annotations.ApiParam;
import io.swagger.jaxrs.*;

import com.sun.jersey.multipart.FormDataParam;
import javax.validation.constraints.*;

import com.uber.athenax.backend.api.ExtendedJobDefinition;
import com.uber.athenax.backend.api.InlineResponse200;
import com.uber.athenax.backend.api.JobDefinition;
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

@Path("/jobs")


@io.swagger.annotations.Api(description = "the jobs API")
@javax.annotation.Generated(value = "io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2018-03-06T14:54:29.251+05:30")
public class JobsApi  {
   private final JobsApiService delegate = JobsApiServiceFactory.getJobsApi();

    @POST
    @Path("/new-jobs")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "Obtain a new UUID for a job", notes = "", response = InlineResponse200.class, tags={ "jobs",  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "successful operation", response = InlineResponse200.class) })
    public Response allocateNewJob(
        @Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.allocateNewJob(securityContext);
    }
    @GET
    @Path("/{jobUUID}")
    
    
    @io.swagger.annotations.ApiOperation(value = "Get the detail information about a job", notes = "", response = JobDefinition.class, tags={ "jobs",  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "successful operation", response = JobDefinition.class) })
    public Response getJob(
        @ApiParam(value = "UUID of the job",required=true) @PathParam("jobUUID") UUID jobUUID,
        @Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getJob(jobUUID,securityContext);
    }
    @GET
    
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "List all jobs", notes = "", response = ExtendedJobDefinition.class, responseContainer = "List", tags={ "jobs",  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "successful operation", response = ExtendedJobDefinition.class, responseContainer = "List") })
    public Response listJob(
        @Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.listJob(securityContext);
    }
    @DELETE
    @Path("/{jobUUID}")
    
    
    @io.swagger.annotations.ApiOperation(value = "Remove a defintion of a job", notes = "", response = void.class, tags={ "jobs",  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "successful operation", response = void.class) })
    public Response removeJob(
        @ApiParam(value = "UUID of the job",required=true) @PathParam("jobUUID") UUID jobUUID,
        @Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.removeJob(jobUUID,securityContext);
    }
    @POST
    @Path("/{jobUUID}")
    
    
    @io.swagger.annotations.ApiOperation(value = "Update the definition of a job. The update might lead to starting / stopping jobs", notes = "", response = void.class, tags={ "jobs" })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "successful operation", response = void.class) })
    public Response updateJob(
        @ApiParam(value = "UUID of the job",required=true) @PathParam("jobUUID") UUID jobUUID,
        @ApiParam(value = "The application to be run" ,required=true) JobDefinition body,
        @Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.updateJob(jobUUID,body,securityContext);
    }
}
