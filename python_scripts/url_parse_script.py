from urllib.parse import urlsplit,parse_qs


url= """https://www.mydomain.com/page-name?utm_con
 tent=textlink&utm_medium=social&utm_source=twit
 ter&utm_campaign=fallsale"""

split_url=urlsplit(url)

print(split_url)
print(f"Scheme:{split_url.scheme}")
print(f"netloc:{split_url.netloc}")
print(f"path:{split_url.path}")
print(f"query:{split_url.query}")

params=parse_qs(split_url.query)

print(f"utm_content:{params['utm_medium']}")

print(params)



print(type(split_url))