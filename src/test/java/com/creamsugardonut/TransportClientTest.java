package com.creamsugardonut;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.lang3.builder.ToStringBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TransportClientTest {

	private TransportClient client;

	@Before
	public void before() throws Exception {
		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "creamsugardonut").build();

		// on startup
		client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
	}

	@After
	public void after() {
		client.close();
	}

	@Test
	public void testPut() throws Exception {
		client.prepareIndex("megacorp", "employee", "1") //
				.setSource("first_name", "John") //
				.setSource("last_name", "Smith") //
				.setSource("age", 25) //
				.setSource("about", "I love to go rock climbing") //
				.setSource("interests", new String[]{"sports", "music"}).execute();
	}

	@Test
	public void testBulk() throws Exception {

		IndexRequest indexRequest = new IndexRequest("megacorp", "employee", "2");
		indexRequest.source("first_name", "Jane") //
				.source("last_name", "Smith") //
				.source("age", 32) //
				.source("about", "I like to collect rock albums") //
				.source("interests", new String[]{"music"});
		client.prepareBulk().add(indexRequest).execute();

		client.prepareIndex("megacorp", "employee", "2") //
				.setSource("first_name", "Jane") //
				.setSource("last_name", "Smith") //
				.setSource("age", 32) //
				.setSource("about", "I like to collect rock albums") //
				.setSource("interests", new String[]{"music"});

		client.prepareIndex("megacorp", "employee", "3") //
				.setSource("first_name", "Douglas") //
				.setSource("last_name", "Fir") //
				.setSource("age", 35) //
				.setSource("about", "I like to build cabinets") //
				.setSource("interests", new String[]{"forestry"});
	}

	@Test
	public void testDelete() throws Exception {
		DeleteRequest deleteRequest = new DeleteRequest("megacorp", "employee", "2");
		client.delete(deleteRequest);

		deleteRequest = new DeleteRequest("megacorp", "employee", "3");
		client.delete(deleteRequest);
	}

	@Test
	public void testGet() throws Exception {

		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "creamsugardonut").build();

		// on startup
		Client client = new TransportClient(settings)
				.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

		String indexName = "cars";
		GetRequest request = new GetRequest(indexName);
		ActionFuture<GetResponse> response = client.get(request);

		System.out.println("fields = " + response.get().getId());

		// on shutdown
		client.close();
	}
}