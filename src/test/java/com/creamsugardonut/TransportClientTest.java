package com.creamsugardonut;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.lang3.builder.ToStringBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.search.MatchQuery;
import org.elasticsearch.index.search.MultiMatchQuery;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

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
        testDelete();

		Map<String, Object> map = new HashMap<>();
		map.put("first_name", "John");
		map.put("last_name", "Smith");
		map.put("age", 25);
		map.put("about", "I love to go rock climbing");
		map.put("interests", new String[]{"sports", "music"});
		client.prepareIndex("megacorp", "employee", "1") //
				.setSource(map) //
				.execute();
	}

	@Test
	public void testBulk() throws Exception {
		Map<String, Object> map1 = new HashMap<>();
		map1.put("first_name", "Jane");
		map1.put("last_name", "Smith");
		map1.put("age", 32);
		map1.put("about", "I like to collect rock albums");
		map1.put("interests", new String[]{"music"});
		IndexRequest indexRequest1 = new IndexRequest("megacorp", "employee", "2");
		indexRequest1.source(map1);

		Map<String, Object> map2 = new HashMap<>();
		map2.put("first_name", "Douglas");
		map2.put("last_name", "Fir");
		map2.put("age", 35);
		map2.put("about", "I like to build cabinets");
		map2.put("interests", new String[]{"forestry"});
		IndexRequest indexRequest2 = new IndexRequest("megacorp", "employee", "3");
		indexRequest2.source(map2);

		client.prepareBulk().add(indexRequest1).add(indexRequest2).execute();
	}

	@Test
	public void testDelete() throws Exception {
		DeleteRequest deleteRequest = new DeleteRequest("megacorp", "employee", "1");
		client.delete(deleteRequest);

		deleteRequest = new DeleteRequest("megacorp", "employee", "2");
		client.delete(deleteRequest);

		deleteRequest = new DeleteRequest("megacorp", "employee", "3");
		client.delete(deleteRequest);
	}

	@Test
	public void testGet() throws Exception {
		ActionFuture<GetResponse> response = client.prepareGet("megacorp", "employee", "1").execute();
		System.out.println("fields = " + response.get().getSourceAsString());
	}

	@Test
	public void testSearch() throws Exception {
		ActionFuture<SearchResponse> response = client.prepareSearch("megacorp") //
				.setTypes("employee") //
				.execute();

		System.out.println("fields = " + response.get());
	}

    @Test
    public void testSearch2() throws Exception {
        MatchQueryBuilder matchQuery = QueryBuilders.matchQuery("last_name", "Smith");
        ActionFuture<SearchResponse> response = client.prepareSearch("megacorp") //
                .setTypes("employee") //
                .setQuery(matchQuery)
                .execute();

        System.out.println("fields = " + response.get());
    }
}