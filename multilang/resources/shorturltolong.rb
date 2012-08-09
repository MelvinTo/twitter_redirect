require "./storm"
require 'rubygems'
require 'httpclient'
require 'rest_client'

$url_keywords = [
      "http://t.co",
      "http://t.cn",
      "http://bit.ly"
]



class ShortURLToLongBolt < Storm::Bolt
  def process(tup)
    message = tup.values[0]
    original_message = message
    message.sub(/(.*) (http[^ ]*)(.*)/) {
       |match| original_message = $1 + " " + get_long_url($2) + $3
    }
    emit([original_message])
  end
  
  def get_long_url(url)
      $url_keywords.each do |keyword|
        if url.match(keyword)
          client = HTTPClient.new
          result = client.head(url)
          return get_long_url result.header['Location'][0]
        end
      end

      return url
  end
end

ShortURLToLongBolt.new.run