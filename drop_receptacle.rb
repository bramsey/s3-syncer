#!/usr/bin/env ruby

require 'rubygems'
require 'yaml'
require 'aws-sdk'
require 'observer'
require 'json'

class Watcher
  include Observable 

  def initialize(directory, initial_remote_state, interval)
    @watched_directory = directory
    begin
      @initial_state = JSON.parse(initial_remote_state) || {'names' => {}, 'etags' => {}}
    rescue
      @initial_state = {'names' => {}, 'etags' => {}}
    end
    @interval = interval
  end

  def run
    prev_dir_state = @initial_state

    loop do
      curr_dir_state = @watched_directory.load_state
      actions = compare_states(prev_dir_state, curr_dir_state)
      queue_actions(actions)
      prev_dir_state = curr_dir_state

      sleep @interval
    end
  end

  private

    def queue_actions(actions)
      unless actions.empty?
        changed # trigger observer change
        actions.each do |action|
          notifiy_observers_(action.to_json)
        end
      end
    end

    # returns an array of the elements in collection not present in other_collection
    def difference(collection, other_collection)
      if collection.is_a?(Hash) && other_collection.is_a?(Hash)
        keys = collection.each_key.to_a
        return keys.reject {|key| other_collection[key]}.map {|key| collection[key]}
      elsif collection.is_a?(Array) && other_collection.is_a?(Array) 
        return collection.reject {|item| other_collection.include?(item)}
      end
      [] # incompatible collections passed
    end

    # returns an array of the elements in collection also present in other_collection
    def intersection(collection, other_collection)
      if collection.is_a?(Hash) && other_collection.is_a?(Hash)
        keys = collection.each_key.to_a
        return keys.select {|key| other_collection[key]}.map {|key| collection[key]}
      elsif collection.is_a?(Array) && other_collection.is_a?(Array) 
        return collection.select {|item| other_collection.include?(item)}
      end
      [] # incompatible collections passed
    end

    def compare_states(prev, curr)
      actions = []

      new_names = difference(curr['names'], prev['names'])
      new_etags = difference(curr['etags'], prev['etags'])
      files_to_add = intersection(new_names, new_etags)
      unless files_to_add.empty?
        files_to_add.each do |file|
          actions.push({'action' => 'add', 'file' => file})
        end
      end

      removed_names = difference(prev['names'], curr['names'])
      removed_etags = difference(prev['etags'], curr['etags'])
      files_to_remove = intersection(removed_names, removed_etags)
      unless files_to_remove.empty?
        files_to_remove.each do |file|
          actions.push({'action' => 'remove', 'name' => file['name']})
        end
      end

      unchanged_etags = intersection(prev['etags'], curr['etags'])
      unchanged_names = intersection(prev['names'], curr['names'])
      files_to_rename = difference(unchanged_etags, unchanged_names)
      unless files_to_rename.empty?
        files_to_rename.each do |file|
          old_name = prev['etags'][file['etag']]['name']
          new_name = curr['etags'][file['etag']]['name']
          actions.push({'action' => 'rename',
                        'from' => old_name,
                        'to' => new_name})
        end
      end

      files_to_modify = difference(unchanged_names, unchanged_etags)
      unless files_to_modify.empty?
        files_to_modify.each do |file|
          actions.push({'action' => 'add', 'file' => file})
        end
      end
      actions
    end
end

class LocalDirectory

  def initialize(path)
    @dir_path = path
  end

  def load_state
    dir_state = {'names' => {}, 'etags' => {}}
    prev_dir = Dir.pwd
    Dir.chdir(@dir_path)
    files = Dir.glob('**/*')

    files.each do |file|
      if File.file?(file) && File.readable?(file)
        file_path = Dir.pwd + '/' + file
        etag = %x[md5 #{file_path}].split('=')[1].strip
        modified = File.stat(file).mtime
        file_info = {'name' => file, 'etag' => etag, 'modified' => modified}
        dir_state['names'][file_info['name']] = file_info
        dir_state['etags'][file_info['etag']] = file_info
      end
    end

    Dir.chdir(prev_dir)

    dir_state
  end
end

class S3Bucket

  def initialize(bucket_name, access_key_id, secret_access_key)
    @bucket_name = bucket_name
    AWS.config(
      'access_key_id' =>  access_key_id, 
      'secret_access_key' => secret_access_key
    )
    @s3_instance = AWS::S3.new
    begin
      @bucket = @s3_instance.buckets[@bucket_name]
    rescue
      puts "Error loading bucket #{@bucket_name}."
    end
  end

  def load_state
    dir_state = {'names' => {}, 'etags' => {}}
    begin
      @bucket.objects.each do |obj|
        head = obj.head
        file_info = {'name' => obj.key, 'etag' => head.etag, 'modified' => head.last_modified} 
        dir_state['names'][file_info['name']] = file_info
        dir_state['etags'][file_info['etag']] = file_info
      end
    rescue
      puts "Error loading file information from bucket #{@bucket_name}."
    end

    dir_state
  end

  def add_file(file_name)
    begin
      puts "Uploading file #{file_name} to bucket #{@bucket_name}."
      file = File.open(file_name, 'r')
      obj = @bucket.objects[file_name]
      obj.write(:content_length => file.size) do |buffer, bytes|
        buffer.write(file.read(bytes))
      end
    rescue
      puts "Error uploading #{file_name} to bucket #{@bucket_name}."
    end
  end

  def rename_file(old_name, new_name)
    begin
      puts "Renaming #{old_name} to #{new_name}"
      @bucket.objects[old_name].move_to(new_name)
    rescue
      puts "Error renaming #{old_name} to #{new_name}"
    end
  end

  def remove_file(file_name)
    begin
      puts "Deleting file #{file_name} from bucket #{@bucket_name}"
      @bucket.objects[file_name].delete
    rescue
      puts "Error deleting #{file_name} from bucket #{@bucket_name}"
    end
  end
end

class Dispatcher

  def initialize(watcher, bucket)
    watcher.add_observer(self)
    @s3_bucket = bucket
  end

  def update(json_action)
    action = JSON.parse(json_action)

    # do appropriate action here in new thread
  end
end


def load_config
  config = YAML.load_file('config.yaml')
  @access_key_id = config['s3_info']['access_key_id']
  @secret_access_key = config['s3_info']['secret_access_key']
  @bucket_name = config['s3_info']['bucket']

  # set local sync dir based on config.
  sync_dir = config['s3_info']['sync_dir']
  Dir.chdir(Dir.pwd + '/' + sync_dir)

end




load_config
local_directory = LocalDirectory.new(Dir.pwd)
bucket = S3Bucket.new(@bucket_name, @access_key_id, @secret_access_key)
bucket_state = bucket.load_state.to_json
local_watcher = Watcher.new(local_directory, bucket_state, 1)
#s3_watcher = Watcher.new(bucket, 30)
local_watcher.run
