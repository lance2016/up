CREATE FOREIGN TABLE public.test1  
(
  	a VARCHAR(255),
	b VARCHAR(255),
	c VARCHAR(255),
	d VARCHAR(255)
)
 SERVER gsmpp_server
 OPTIONS 
(
	LOCATION 'gsfs://10.20.139.4:8082/test1.dat', 
	FORMAT 'CSV' ,
	DELIMITER E'\x05',
	quote E'\x1b',
	null '',
	ENCODING 'utf8',
	HEADER 'false',
	FILL_MISSING_FIELDS 'true',
	IGNORE_EXTRA_DATA 'true'
)
LOG INTO product_info_err 
PER NODE REJECT LIMIT '10000000';