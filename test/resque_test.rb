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

    Resque.push_with_priority(:priority_jobs, job, 75)

    # we actually store 1000 minus the priority
    assert_equal "925", Resque.redis.zscore('queue:priority_jobs', Resque.encode(job))

  end

  def test_push
    job = { :class => SomePriorityJob, :args => [] }

    Resque.push_with_priority(:priority_jobs, job, 75)

    # subsequent pushes to this queue should work correctly and be given a default priority
    new_job = job.merge(:args => [ 'must', 'be', 'different'])
    Resque.push(:priority_jobs, new_job)

    assert_equal "500", Resque.redis.zscore('queue:priority_jobs', Resque.encode(new_job))

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


  # stolen/modified from resque peek test
  def test_peek_no_priority
    Resque.push(:non_priority_jobs, { 'name' => 'chris' })
    Resque.push(:non_priority_jobs, { 'name' => 'bob' })
    Resque.push(:non_priority_jobs, { 'name' => 'mark' })

    assert_equal({ 'name' => 'chris' }, Resque.peek(:non_priority_jobs))



    
    assert_equal({ 'name' => 'bob' }, Resque.peek(:non_priority_jobs, 1, 1))

    assert_equal([{ 'name' => 'bob' }, { 'name' => 'mark' }], Resque.peek(:non_priority_jobs, 1, 2))
    assert_equal([{ 'name' => 'chris' }, { 'name' => 'bob' }], Resque.peek(:non_priority_jobs, 0, 2))
    assert_equal([{ 'name' => 'chris' }, { 'name' => 'bob' }, { 'name' => 'mark' }], Resque.peek(:non_priority_jobs, 0, 3))
    assert_equal({ 'name' => 'mark' }, Resque.peek(:non_priority_jobs, 2, 1))
    assert_equal nil, Resque.peek(:non_priority_jobs, 3)
    assert_equal [], Resque.peek(:non_priority_jobs, 3, 2)
  end

  # stolen/modified from resque peek test
  def test_peek_with_priority
    Resque.push_with_priority(:priority_jobs, { 'name' => 'chris' }, 100)
    Resque.push_with_priority(:priority_jobs, { 'name' => 'bob' }, 50)
    Resque.push_with_priority(:priority_jobs, { 'name' => 'mark' }, 49)

    assert_equal({ 'name' => 'chris' }, Resque.peek(:priority_jobs))




    assert_equal({ 'name' => 'bob' }, Resque.peek(:priority_jobs, 1, 1))

    assert_equal([{ 'name' => 'bob' }, { 'name' => 'mark' }], Resque.peek(:priority_jobs, 1, 2))
    assert_equal([{ 'name' => 'chris' }, { 'name' => 'bob' }], Resque.peek(:priority_jobs, 0, 2))
    assert_equal([{ 'name' => 'chris' }, { 'name' => 'bob' }, { 'name' => 'mark' }], Resque.peek(:priority_jobs, 0, 3))
    assert_equal({ 'name' => 'mark' }, Resque.peek(:priority_jobs, 2, 1))
    assert_equal nil, Resque.peek(:priority_jobs, 3)
    assert_equal [], Resque.peek(:priority_jobs, 3, 2)
  end

  def test_clean_priority
    # reminder that we return 1000 minus the priority.  after the priority has been cleaned, a lower number is 'higher'

    assert_equal 0, Resque.send(:clean_priority, :highest)
    assert_equal 250, Resque.send(:clean_priority, :high)
    assert_equal 500, Resque.send(:clean_priority, :normal)
    assert_equal 750, Resque.send(:clean_priority, :low)
    assert_equal 1000, Resque.send(:clean_priority, :lowest)

    assert_equal 991, Resque.send(:clean_priority, 9)
    assert_equal 923, Resque.send(:clean_priority, 77)
    assert_equal 963, Resque.send(:clean_priority, 37)
    assert_equal 906, Resque.send(:clean_priority, 94)

    assert_equal 1000, Resque.send(:clean_priority, nil)
    assert_equal 1000, Resque.send(:clean_priority, Hash.new)

  end

  def test_is_priority_queue?

    Resque.push_with_priority(:priority_jobs, { :class => SomePriorityJob, :args => [ ] })
    Resque.push(:non_priority_jobs, { :class => SomeNonPriorityJob, :args => [ ] })

    assert Resque.is_priority_queue?(:priority_jobs)
    assert !Resque.is_priority_queue?(:non_priority_jobs)

  end

  def test_priority
    job = { :class => SomePriorityJob, :args => [ 'hi' ] }
    Resque.push_with_priority(:priority_jobs, job, 77)

    assert_equal 77, Resque.priority(:priority_jobs, job)
  end

  def test_priority_enabled?
    Resque.redis.del 'priority_queues'

    Resque.redis.sadd 'priority_queues', 'good_queue'

    assert !Resque.priority_enabled?('bad_queue')
    assert !Resque.priority_enabled?(:bad_queue)

    assert Resque.priority_enabled?('good_queue')
    assert Resque.priority_enabled?(:good_queue)

  end

end