require File.expand_path(File.dirname(__FILE__) + '/test_helper')

class ExtendTest < Test::Unit::TestCase

  class TestJob
    extend Resque::Plugins::PriorityQueue
  end

  def test_priority_setter_and_getter
    assert_nil TestJob.priority    
    TestJob.priority = 101
    assert_equal 101, TestJob.priority
  end

end