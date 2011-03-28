require File.expand_path(File.dirname(__FILE__) + '/test_helper')

class JobTest < Test::Unit::TestCase

  def setup
    Resque::Plugins::PriorityQueue.enable!
  end

  class ::SomePriorityJob

    def self.after_enqueue_do_something
      @did_something = true
    end

    def self.perform(*args)
    end
  end


  def test_create_with_priority
    @worker = Resque::Worker.new(:priority_jobs)

    Resque::Job.create_with_priority(:priority_jobs, SomePriorityJob, 5000)

    assert_equal "5000", Resque.redis.zscore("queue:priority_jobs", Resque.encode(:class => 'SomePriorityJob', :args => []))

    @worker.work(0)

    assert ::SomePriorityJob.instance_variable_get(:@did_something)
  end

end