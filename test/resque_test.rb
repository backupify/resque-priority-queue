require File.expand_path(File.dirname(__FILE__) + '/test_helper')

class JobTest < Test::Unit::TestCase

  def setup
    Resque::Plugins::PriorityQueue.enable!
  end

  class ::SomePriorityJob
    def self.perform(*args); end
  end

  class ::SomeNonPriorityJob
    def self.perform(*args); end
  end

  def test_push_with_priority
    job = { :class => SomePriorityJob, :args => [] }

    Resque.push_with_priority(:priority_jobs, job, 5000)

    assert_equal "5000", Resque.redis.zscore('queue:priority_jobs', Resque.encode(job))

  end

  def test_push
    job = { :class => SomePriorityJob, :args => [] }

    Resque.push_with_priority(:priority_jobs, job, 5000)

    # subsequent pushes to this queue should work correctly and be given a default priority
    new_job = job.merge(:args => [ 'must', 'be', 'different'])
    Resque.push(:priority_jobs, new_job)

    assert_equal "50", Resque.redis.zscore('queue:priority_jobs', Resque.encode(new_job))

    # a regular push to a queue that hasn't been initialized with priority should be a normal set
    non_priority_job = { :class => SomeNonPriorityJob, :args => [] }
    Resque.push(:non_priority_jobs, non_priority_job)

    assert_equal 'list', Resque.redis.type('queue:non_priority_jobs')
  end

  def test_pop
    # pop should return elements from priority queues in decreasing order of priority
    5.times { |i| Resque.push_with_priority(:priority_jobs_2, { :class => SomePriorityJob, :args => [ "#{i}" ] }, i) }

    last_priority = nil
    5.times do
      job = Resque.pop(:priority_jobs_2)
      assert last_priority == nil || job['args'].first.to_i < last_priority
      last_priority = job['args'].first.to_i
    end

    # pop should still work fine with normal list-backed queues
    non_priority_job = { :class => SomePriorityJob, :args => [] }

    Resque.push(:non_priority_jobs_2, non_priority_job)

    assert_equal({ 'class' => 'SomePriorityJob', 'args' => [] }, Resque.pop(:non_priority_jobs_2))

  end

end