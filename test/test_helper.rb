require 'rubygems'
require 'test/unit'
require 'mocha'
require 'resque'

dir = File.dirname(File.expand_path(__FILE__))
$LOAD_PATH.unshift dir + '/../lib'
$TESTING = true

require 'resque/plugins/priority_queue'

# start our own redis when the tests start,
# kill it when they end
at_exit do
  next if $!

  if defined?(MiniTest)
    exit_code = MiniTest::Unit.new.run(ARGV)
  else
    exit_code = Test::Unit::AutoRunner.run
  end

  pid = `ps -e -o pid,command | grep [r]edis-test`.split(" ")[0]
  puts "Killing test redis server..."
  `rm -f #{dir}/dump.rdb`
  Process.kill("KILL", pid.to_i)
  exit exit_code
end

puts "Starting redis for testing at localhost:9736..."
`redis-server #{dir}/redis-test.conf`
Resque.redis = '127.0.0.1:9736'
