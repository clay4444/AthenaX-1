/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.athenax.backend.api.impl;

import com.mydashboard.athena.catalog.impl.MydashboardAthenaCatalogProvider;
import com.uber.athenax.backend.api.JobDefinition;
import com.uber.athenax.backend.api.JobDefinitionDesiredstate;
import com.uber.athenax.backend.api.JobDefinitionResource;
import com.uber.athenax.backend.api.JobsApiService;
import com.uber.athenax.backend.api.NotFoundException;
import com.uber.athenax.backend.api.client.ApiException;
import com.uber.athenax.backend.api.client.JobsApi;
import com.uber.athenax.backend.server.ServerContext;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import java.io.IOException;
import java.util.Collections;
import java.util.UUID;

@javax.annotation.Generated(
    value = "io.swagger.codegen.languages.JavaJerseyServerCodegen",
    date = "2017-09-22T14:43:25.370-07:00")
public class JobsApiServiceImpl extends JobsApiService {
  private static final Logger LOG = LoggerFactory.getLogger(JobsApiServiceImpl.class);
  public static final int INVALID_REQUEST = HttpStatus.SC_INTERNAL_SERVER_ERROR;
  private final ServerContext ctx;

  public JobsApiServiceImpl(ServerContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public Response allocateNewJob(SecurityContext securityContext) throws NotFoundException {
	  try {
		  LOG.info("Allocate new job with default values... START...");
		  JobsApi api = new JobsApi();
		  UUID uuid = ctx.jobManager().newJobUUID();
		  /*String uuidStr = ctx.jobManager().newJobUUID().toString();*/
	        JobDefinitionDesiredstate state = new JobDefinitionDesiredstate()
	            .clusterId("CID-62142d19-3125-4736-8a36-726c81a01fbf")
	            .resource(new JobDefinitionResource().vCores(1L).memory(2048L));
	        JobDefinition job = new JobDefinition()
	            .query("SELECT * FROM input.foo")
	            .addDesiredStateItem(state);
	        api.updateJob(UUID.fromString(uuid.toString()), job);
	        LOG.info("Allocate new job with default values... END...");
	        return Response.ok().entity(
	                Collections.singletonMap("job-uuid", uuid)
	            ).build();
	} catch (Exception ex) {
		ex.printStackTrace();
	}
    /*return Response.ok().entity(
        Collections.singletonMap("job-uuid", ctx.jobManager().newJobUUID())
    ).build();*/
      return null;
  }

  @Override
  public Response getJob(UUID jobUUID, SecurityContext securityContext) throws NotFoundException {
    try {
    LOG.info("Get Job... START...");
      JobDefinition job = ctx.jobManager().getJob(jobUUID);
      if (job == null) {
    	  LOG.info("Get Job... END...");
        return Response.status(Response.Status.NOT_FOUND).build();
      } else {
    	  LOG.info("Get Job... END...");
        return Response.ok(job).build();
      }
    } catch (IOException e) {
      throw new NotFoundException(INVALID_REQUEST, e.getMessage());
    }
  }

  @Override
  public Response listJob(SecurityContext securityContext) throws NotFoundException {
	  LOG.info("List Job... START...");
    try {
      return Response.ok(ctx.jobManager().listJobs()).build();
    } catch (IOException e) {
      throw new NotFoundException(INVALID_REQUEST, e.getMessage());
    }
  }

  @Override
  public Response removeJob(UUID jobUUID, SecurityContext securityContext) throws NotFoundException {
    try {
      ctx.jobManager().removeJob(jobUUID);
    } catch (IOException e) {
      throw new NotFoundException(INVALID_REQUEST, e.getMessage());
    }
    return Response.ok().build();
  }

  @Override
  public Response updateJob(
      UUID jobUUID,
      JobDefinition body,
      SecurityContext securityContext) throws NotFoundException {
    try {
      ctx.jobManager().updateJob(jobUUID, body);
    } catch (IOException e) {
      throw new NotFoundException(INVALID_REQUEST, e.getMessage());
    }
    return Response.ok().build();
  }
}
