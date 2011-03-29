require File.expand_path(File.dirname(__FILE__) + '/test_helper')

class JobTest < Test::Unit::TestCase

  def setup
    Resque::Plugins::PriorityQueue.enable!

    Resque.remove_queue(:priority_jobs)
    Resque.remove_queue(:non_priority_jobs)
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
    5.times { |i| Resque.push_with_priority(:priority_jobs, { :class => SomePriorityJob, :args => [ "#{i}" ] }, i) }

    last_priority = nil
    5.times do
      job = Resque.pop(:priority_jobs)
      assert last_priority == nil || job['args'].first.to_i < last_priority
      last_priority = job['args'].first.to_i
    end

    # pop should still work fine with normal list-backed queues
    non_priority_job = { :class => SomePriorityJob, :args => [] }

    Resque.push(:non_priority_jobs_2, non_priority_job)

    assert_equal({ 'class' => 'SomePriorityJob', 'args' => [] }, Resque.pop(:non_priority_jobs_2))

  end

  def test_size
    # size should work with both zsets and lists

    7.times { |i| Resque.push_with_priority(:priority_jobs, { :class => SomePriorityJob, :args => [ "#{i}" ] }, i) }
    9.times { |i| Resque.push(:non_priority_jobs, { :class => SomeNonPriorityJob, :args => ["#{i}"] })}

    assert_equal 7, Resque.size(:priority_jobs)
    assert_equal 9, Resque.size(:non_priority_jobs)
  end

  def test_sym_to_priority

    assert_equal 100, Resque.send(:sym_to_priority, :high)
    assert_equal 50, Resque.send(:sym_to_priority, :normal)
    assert_equal 0, Resque.send(:sym_to_priority, :low)

    assert_equal 9999, Resque.send(:sym_to_priority, 9999)
    assert_equal 7777, Resque.send(:sym_to_priority, '7777')
    assert_equal 3.14, Resque.send(:sym_to_priority, 3.14)
    assert_equal 6, Resque.send(:sym_to_priority, '6.28')

    assert_equal 0, Resque.send(:sym_to_priority, nil)
    assert_equal 0, Resque.send(:sym_to_priority, Hash.new)

  end

  def test_is_priority_queue?

    Resque.push_with_priority(:priority_jobs, { :class => SomePriorityJob, :args => [ ] })
    Resque.push(:non_priority_jobs, { :class => SomeNonPriorityJob, :args => [ ] })

    assert Resque.is_priority_queue?(:priority_jobs)
    assert !Resque.is_priority_queue?(:non_priority_jobs)

  end

end