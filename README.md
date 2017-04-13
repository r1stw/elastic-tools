Elastic tools
========================

Working with elasticsearch python library? Tired of creating queries as huge and totally unreadable plain json objects? Want to create
requests and interpret result intuitively? We may have a solution **just for you.**

Featuring:
  * Simplified request writing
  * Awesome getter functions
  * Axis and axis iterator
  * Easier connections handilng
  
What can it do? Let's see:

Basics
========================
What would you have to write if you use elasticsearch:


    x = {
	   'aggs': {
		  'filtered': {
			  'filter': {
				  'bool': {
					  'must': [{
						  'term': {
							  'field1': 'value1'
						  }
					  }, {
						  'terms': {
							  'field2': ['value2', 'value3', 'value4']
						  }
					  }]
				  }
			  },
			  'aggs': {
				  'sort_by_keys': {
					  'terms': {
						  'field': 'field3'
					  },
					  'aggs': {
					  	  'unique_field4_values': {
						  	'cardinality': {
								'field': 'field4'
							}
						  }
					  
					  }
				  }
			  }
		  }
	  }
    }
  
  Same request written using elastic-tools:
  

  
      import elastictools.request as req 
      
      x = req.request(filtered=req.agg_filter(
            req.flt_and(
              req.flt_eq("field1", "value1"),
              req.flt_eq("field2", ["value2", "value3", "value4"])
           ),
           sort_by_keys=req.agg_terms("field3",
	                              unique_field4_values=req.agg_cardinality("field4")
	   )
         ))
       
Looks way better, doesn't it?

Getters
=======
Say you have executed your request and now you want to know the value of "unique_field4_values" aggregation in first bucket. What you
would have to do in elasticsearch:

	x = response['aggregations']['filtered']['sort_by_keys']['buckets'][0]['unique_field4_values']['value']


Installation
============

Pretty much what you would expect:
     
     pip install git+https://github.com/skyhound/elastic-tools
     


Contacts
========

Feel free to join this [Discord server](https://discord.gg/sJvDuuj) for support and suggestions.





Documentation
========================
http://elastic-tools.rtfd.io/
