Elastic tools
========================

Working with elasticsearch python library? Tired of creating queries as huge and totally unreadable plain json objects? Want to create
requests and interpret result intuitively? We may have a solution **just for you.**

What can it do? Let's see:

Basics
========================
Say you got a nice request in json form:


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
						  'field': 'field3',
						  'size': 10000,
						  'order': {
							  '_count': 'desc'
						  }
					  }
				  }
			  }
		  }
	  }
    }
  
  Actually, it's not that nice and easy to read and edit, isn't it? What if we try to write this one using elastic-tools?
  

  
      x = req.request({}, filtered=req.agg(
        req.agg_filter(
          req.flt_and(
            req.flt_eq("field1", "value1"),
            req.flt_eq("field2", ["value2", "value3", "value4"])
         )
       ),
       sort_by_keys=req.agg_terms("field3")
       ))
       
Looks way better, doesn't it?


What are you waiting for, just install it already:
     
     pip install git+https://github.com/pleskanovsky/elastic-tools
     


Contacts
========

Feel free to join this [Discord server](https://discord.gg/sJvDuuj) for support.





Documentation
========================
http://elastic-tools.rtfd.io/
