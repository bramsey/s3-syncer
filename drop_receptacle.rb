#!/usr/bin/env ruby

require 'rubygems'
require 'yaml'
require 'aws-sdk'
require 'observer'
require 'json'
require 'time'

class Watcher
  include Observable 

  def initialize(directory, initial_remote_state, interval)
    @watched_directory = directory
    begin
      @initial_state = JSON.parse(initial_remote_state) || {'names' => {}, 'etags' => {}}
    rescue
      @initial_state = {'names' => {}, 'etags' => {}, 'ids' => {}}
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
        notify_observers(actions)
      end
    end

    # returns an array of the elements in collection not present in other_collection
    def difference(collection, other_collection)
      if collection.is_a?(Hash) && other_collection.is_a?(Hash)
        keys = collection.each_key.to_a
        return keys.reject {|key| other_collection[key]}.map {|key| collection[key]}
      elsif collection.is_a?(Array) && other_collection.is_a?(Array) 
        return collection - other_collection
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

      # determine rename actions
      removed_names = difference(prev['names'], curr['names'])
      new_files = difference(curr['ids'], prev['ids'])
      removed_name_hashes = removed_names.map {|file| file['etag']}
      files_to_rename = new_files.select {|file| removed_name_hashes.include?(file['etag'])}
      new_file_hashes = new_files.map {|file| file['etag']}
      renamed_files = removed_names.select {|file| new_file_hashes.include?(file['etag'])}
      files_to_rename.each do |file|
        old_name = removed_names.select {|old_file| old_file['etag'] == file['etag']}[0]['name']
        new_name = file['name']
        unless File.extname(old_name) == '.inprog'
          actions.push({'action' => 'rename',
                        'from' => old_name,
                        'to' => new_name}) 
        end
      end

      # determine add actions
      files_to_add = new_files - files_to_rename
      files_to_add.each do |file|
        unless File.extname(file['name']) == '.inprog'
          actions.push({'action' => 'add', 'file' => file}) 
        end
      end

      # determine remove actions
      files_to_remove = removed_names - renamed_files
      files_to_remove.each do |file|
        actions.push({'action' => 'remove', 'name' => file['name']})
      end

      actions
    end
end

class LocalDirectory

  def initialize(path)
    @dir_path = path
  end

  def LocalDirectory.etag_for(file_path)
    if File.exists?(file_path)
      md5_response = %x[md5 #{file_path}]
      etag = md5_response.split('=')[1]
      etag = etag && etag.strip
    else
      return nil
    end
  end

  def load_state
    dir_state = {'names' => {}, 'etags' => {}, 'ids' => {}}
    prev_dir = Dir.pwd
    Dir.chdir(@dir_path)
    files = Dir.glob('**/*')

    files.each do |file|
      if File.file?(file) && File.readable?(file)
        file_path = Dir.pwd + '/' + file
        etag = LocalDirectory.etag_for(file_path)
        modified = File.stat(file).mtime
        file_info = {'name' => file, 'etag' => etag, 'modified' => modified}
        dir_state['names'][file_info['name']] = file_info
        dir_state['etags'][file_info['etag']] = file_info
        dir_state['ids'][file_info['etag']+file_info['name']] = file_info
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
    dir_state = {'names' => {}, 'etags' => {}, 'ids' => {}}
    begin
      @bucket.objects.each do |obj|
        head = obj.head
        etag = String.class_eval(head.etag) # remove escaping from s3 strings
        file_info = {'name' => obj.key, 'etag' => etag, 'modified' => head.last_modified} 
        dir_state['names'][file_info['name']] = file_info
        dir_state['etags'][file_info['etag']] = file_info
        dir_state['ids'][file_info['etag']+file_info['name']] = file_info
      end
    rescue
      puts "Error loading file information from bucket #{@bucket_name}."
    end

    dir_state
  end

  def add_file(file_info)
    file_name = file_info['name']
    puts "Uploading file #{file_name} to bucket #{@bucket_name}."
    begin
      obj = @bucket.objects[file_name]
      if obj.exists?
        etag = String.class_eval(obj.etag)
        mtime = obj.last_modified
      else
        etag = nil
        mtime = nil
      end
      puts 'hai'
      puts etag.inspect
      unless etag == file_info['etag'] || (mtime && mtime >= Time.parse(file_info['modified']))
        begin
            #obj.write(:file => file_name)
            obj = @bucket.objects[file_name + '.inprog']
            file = File.open(file_name, 'r')
            obj.write(:content_length => file.size) do |buffer, bytes|
              buffer.write(file.read(bytes))
            end
            file.close
            obj.move_to(file_name)
            puts "Done uploading file #{file_name} to bucket #{@bucket_name}"
        rescue
          puts "Error uploading #{file_name} to bucket #{@bucket_name}."
        end
      else
        puts "obj unchanged"
      end
    rescue
      puts "Error reading #{file_name} from bucket #{@bucket_name}."
    end
  end

  def get_file(file_info)
    file_name = file_info['name']
    puts "Downloading file #{file_name} to local folder #{Dir.pwd}"
    begin
      obj = @bucket.objects[file_name]
      local_file_path = File.join(Dir.pwd, file_name)
      local_file_etag = LocalDirectory.etag_for(local_file_path)
      local_file_stats = File.exists?(local_file_path) && File.stat(local_file_path)
      local_file_mtime = local_file_stats && local_file_stats.mtime

      unless local_file_etag == file_info['etag'] || (local_file_mtime && local_file_mtime >= Time.parse(file_info['modified']))
        begin
          temp_file_name = local_file_path + '.inprog'
          file = File.open(temp_file_name, 'w')
          obj.read do |chunk|
            file.write(chunk)
          end
          file.close
          File.rename(temp_file_name, local_file_path)
          puts "Done downloading file #{file_name}"
        rescue
          puts "Error downloading #{file_name} to local folder #{Dir.pwd}"
        end
      else
        puts "file #{file_name} already synced"
      end
    rescue
      puts "Error reading #{file_name} from local folder #{Dir.pwd}"
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
    @bucket = bucket
  end

  def update(actions)
    #action = JSON.parse(json_action)

    actions.each do |action|
      if action['action']
        #case action['action']
        #when 'add'
        #  @bucket.add_file(action['file'])
        #when 'rename'
        #  @bucket.rename_file(action['from'], action['to'])
        #when 'remove'
        #  @bucket.remove_file(action['name'])
        #end
        puts action
      end
    end
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
local_state = local_directory.load_state
bucket.add_file(local_state['names']['readme.txt'])
local_watcher = Watcher.new(local_directory, local_directory.load_state.to_json, 1)
s3_watcher = Watcher.new(bucket, bucket.load_state.to_json, 1)
local_dispatcher = Dispatcher.new(local_watcher, bucket)
remote_dispatcher = Dispatcher.new(s3_watcher, local_directory)

local_thread = Thread.new { local_watcher.run }
#remote_thread = Thread.new { s3_watcher.run }
local_thread.join
remote_thread.join
