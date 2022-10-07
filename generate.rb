require 'fileutils'
require 'zlib'
require 'time'
require 'set'

ANSIBLE_DIR = "/Users/jbrayton/Documents/ansible-scripts"
LOG_DIR = "/Users/jbrayton/Documents/ansible-scripts/retriever_logs"

FileUtils.rm_rf(LOG_DIR)

Dir.chdir(ANSIBLE_DIR){
  `./run_playbook.bash ft-prd fetch_retriever_logs.yml`
}

ERROR_TYPE_CONNECTION = -1
ERROR_TYPE_READABILITY = -2

def parse_time(time_str)
	return Time.parse(time_str)
end

class Error
	
	attr_accessor :url
	attr_accessor :error_type
	attr_accessor :retriever
	attr_accessor :time
	
	def initialize( url, error_type, retriever, time )
		@url = url
		@error_type = error_type
		@retriever = retriever
		@time = time
	end

end


#			if /E, \[([^ ]+) [^\]+\] ERROR -- \: Connection error for article with url (.*)/.match(line)

num_success = 0
all_errors = Array.new
successfully_retrieved_urls = Set.new
Dir["#{LOG_DIR}/**/*.log*"]. each do |item|
	retriever = item.split('/')[6]
	reader = nil
	if item.include?(".gz")
		reader = Zlib::GzipReader.open(item)
	else
		reader = File.open(item)
	end
	if !reader.nil?
		reader.each_line do |line|
			if /E, \[([^ ]+) \#[0-9]+\] ERROR -- \: Connection error for article with url (.*)/.match(line)
				parsed_time = parse_time($1)
				all_errors.push(Error.new($2, ERROR_TYPE_CONNECTION, retriever, parsed_time))
			end
			if /E, \[([^ ]+) \#[0-9]+\] ERROR -- \: Readability error for (.*)/.match(line)
				parsed_time = parse_time($1)
				all_errors.push(Error.new($2, ERROR_TYPE_READABILITY, retriever, parsed_time))
			end
			if /E, \[([^ ]+) \#[0-9]+\] ERROR -- \: Unexpected status code for (.*): ([0-9]+)/.match(line)
				parsed_time = parse_time($1)
				all_errors.push(Error.new($2, $3.to_i, retriever, parsed_time))
			end
			if /I, \[[^ ]+ \#[0-9]+\]  INFO -- : Successfully generated webpage text for (.*)/.match(line)
				successfully_retrieved_urls.add($1)
			end
		end
	end
	reader.close
end

all_errors = all_errors.sort_by(&:url)

# Limit to errors encountered within the past 24 hours and those that were not later
# retrieved successfully.
earliest_time = Time.now - 86400
filtered_errors = Array.new
all_errors.each do |error|
	if error.time > earliest_time and !successfully_retrieved_urls.include?(error.url)
		filtered_errors.push(error)
	end
end
all_errors = filtered_errors

error_types = Set.new
all_errors.each do |error|
	error_types.add(error.error_type)
end
error_types = error_types.to_a.sort

error_types.each do |error_type|
	title = "Status Code #{error_type}"
	if error_type == ERROR_TYPE_READABILITY
		title = "Parsing / Generation Error"
	elsif error_type == ERROR_TYPE_CONNECTION
		title = "Connection Error"
	end
	puts title
	puts ""
	seen_urls = Set.new
	all_errors.each do |error|
		if error.error_type == error_type and !seen_urls.include?(error.url)
			puts "- #{error.url} (#{error.retriever} at #{error.time}"
			seen_urls.add(error.url)
		end
	end
	puts ""
	puts ""
end


