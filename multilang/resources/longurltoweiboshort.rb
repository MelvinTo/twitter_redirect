require "./storm"
require 'rubygems'
require 'httpclient'
require 'rest-client'

%w(sinatra haml oauth sass json weibo).each { |dependency| require dependency }

Weibo::Config.api_key = "2508311422"
Weibo::Config.api_secret = "fc0501784306ce2a4f2e97ef74b8ee13"

# $weibo_api_key = 2508311422
$sina_shorten_url = "https://api.weibo.com/2/short_url/shorten.json"

class LongURLToWeiboShortBolt < Storm::Bolt
  
  def connect() 
    oauth = Weibo::OAuth.new(Weibo::Config.api_key, Weibo::Config.api_secret)
    request_token = oauth.consumer.get_request_token
    request_url = request_token.authorize_url
    puts request_url
  end
  def process(tup)
    connect()
    message = tup.values[0]
    original_message = message
    message.sub(/(.*) (http[^ ]*)(.*)/) {
       |match| original_message = $1 + " " + shorten_url($2) + $3
    }
    emit([original_message])
  end
  
  def shorten_url(url)
      response = RestClient.get $sina_shorten_url, {:params => {'url_long' => url, 'source' => $weibo_api_key }}
  #    response.body
      json_object = JSON.parse(response.body)
      json_object[0]["url_short"]
  end
  
end
LongURLToWeiboShortBolt.new.run