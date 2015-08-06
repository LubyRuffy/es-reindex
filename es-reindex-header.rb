#!/usr/bin/env ruby
#encoding:utf-8
require 'bundler/setup'
require 'rest-client'
require 'oj'

VERSION = '0.0.8'

STDOUT.sync = true

Oj.default_options = {:mode => :compat}

if ARGV.size == 0 or ARGV[0] =~ /^-(?:h|-?help)$/
  puts "Reindex header of fofa

Usage:

  #{__FILE__} [-f <frame>] [destination_url/]<index>

    - -f - specify frame size to be obtained with one fetch during scrolling
    - optional destination urls default to http://127.0.0.1:9200
\n"
  exit 1
end

def retried_request method, url, data=nil
  while true
    begin
      #puts method, url, data
      return data ?
        RestClient.send(method, url, data) :
        RestClient.send(method, url)
    rescue RestClient::ResourceNotFound # no point to retry
      return nil
    rescue => e
      warn "\nRetrying #{method.to_s.upcase} ERROR: #{e.class} - #{e.message}"
      warn e.response
    end
  end
end

def tm_len l
  t = []
  t.push l/86400; l %= 86400
  t.push l/3600;  l %= 3600
  t.push l/60;    l %= 60
  t.push l
  out = sprintf '%u', t.shift
  out = out == '0' ? '' : out + ' days, '
  out << sprintf('%u:%02u:%02u', *t)
  out
end


dst = nil
durl, didx = '', ''
bulk_op = 'update'
total = 0
t, done = Time.now, 0
frame = 1000

while ARGV[0]
  case arg = ARGV.shift
    when '-f' then frame = ARGV.shift.to_i
    else
      u = arg.chomp '/'
      !dst ? (dst = u) : raise("Unexpected parameter '#{arg}'. Use '-h' for help.")
  end
end

[[dst, durl, didx]].each do |param, url, idx|
  if param =~ %r{^(.*)/(.*?)$}
    url.replace $1
    idx.replace $2
  else
    url.replace 'http://127.0.0.1:9200'
    idx.replace param
  end
end

printf "Reindex '%s/%s' header\n", durl, didx

while true do
  data = retried_request(:post,
      "#{durl}/#{didx}/_search/", %Q|
{
  "_source": [
    "header"
  ],
  "query": {
    "constant_score": {
      "filter": {
        "missing": {
          "field": "header_ok"
        }
      }
    }
  },
  "size": #{frame}
}
      |)
  data = Oj.load data
  break if data['hits']['hits'].empty?
  total = data['hits']['total'].to_i if total==0
  bulk = ''
  data['hits']['hits'].each do |doc|
    base = {'_index' => didx, '_id' => doc['_id'], '_type' => doc['_type']}
    ['_timestamp', '_ttl'].each{|doc_arg|
      base[doc_arg] = doc[doc_arg] if doc.key? doc_arg
    }
    bulk << Oj.dump({bulk_op => base}) + "\n"

    source = doc['_source']
    #puts Oj.dump(source)
    bulk << '{"doc" : ' + Oj.dump(source) + "}\n"
    done += 1
  end
  unless bulk.empty?
    bulk << "\n" # empty line in the end required
    #puts bulk
    retried_request :post, "#{durl}/_bulk", bulk
    #exit
  end

  eta = total * (Time.now - t) / done
  printf "    %u/%u (%.1f%%) done in %s, E.T.A.: %s.\r",
    done, total, 100.0 * done / total, tm_len(Time.now - t), t + eta
end
