require 'json'
require 'open-uri'
require 'net/http'


class TempFetcher
  def initialize
    @account = ENV['CDB_ACCOUNT']
    @api_key = ENV['CDB_API_KEY']
    @forecast_io_key = ENV['FORECAST_IO_KEY']
  end

  def cartodb_query(sql)
    puts "Running query: #{sql}"
    http = Net::HTTP.new("#{@account}.cartodb.com")
    resp = http.post("http://#{@account}.cartodb.com/api/v1/sql", "q=#{URI::encode(sql)}&api_key=#{@api_key}")
    resp.body
  end


  def query_places
    places = JSON::parse cartodb_query("SELECT name1,name2,internalname,population,lat,lon FROM heatwave_cities")
    places['rows'].collect{|p| [p['internalname'], p['lat'].gsub(',','.'), p['lon'].gsub(',','.')]}.reject{|t| t[1] == '' || t[1] == nil}
  end

  def query_city(internal_name, lat, lon)
    puts "Querying weather for city #{internal_name}"
    weather_obj = JSON.parse(open("https://api.forecast.io/forecast/#{@forecast_io_key}/#{lat},#{lon}?units=si").read)
    puts "obtaining temps.."
    times_and_temps = weather_obj['daily']['data'].collect{|d| [d['time'], d['apparentTemperatureMax']] }[1..-1]
    
    query = "INSERT INTO temperatures(internalname, the_geom, temperature, time) VALUES "
    query += times_and_temps.collect{|t| "('#{internal_name}', CDB_LatLng(#{lat},#{lon}), #{t[1]}, to_timestamp(#{t[0]}))" }.join(",")
    query
  end

  def run!
    places = query_places
    update_query = places.collect do |place|
      query_city(*place)
    end

    cartodb_query("TRUNCATE TABLE temperatures;" + update_query.join(";"))
  end
end

t = TempFetcher.new
t.run!
