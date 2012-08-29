#!/usr/bin/env ruby

require 'rubygems'
require 'yaml'
require 'aws-sdk'
require 'observer'

class Watcher
  include Observable

  def initialize(directory, interval)
    @watched_directory = directory
    @interval = interval
  end

  def run
    prev_dir_state = {:names => {}, :etags => {}}

    loop do
      curr_dir_state = @watched_directory.load_state
      compare_states(prev_dir_state, curr_dir_state)
      prev_dir_state = curr_dir_state

      sleep @interval
    end
  end

  private

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
      new_names = difference(curr[:names], prev[:names])
      new_etags = difference(curr[:etags], prev[:etags])
      files_to_add = intersection(new_names, new_etags)
      unless files_to_add.empty?
        puts "files to add: "
        puts files_to_add
      end

      removed_names = difference(prev[:names], curr[:names])
      removed_etags = difference(prev[:etags], curr[:etags])
      files_to_remove = intersection(removed_names, removed_etags)
      unless files_to_remove.empty?
        puts "files to remove: "
        puts files_to_remove
      end

      unchanged_etags = intersection(prev[:etags], curr[:etags])
      unchanged_names = intersection(prev[:names], curr[:names])
      files_to_rename = difference(unchanged_etags, unchanged_names)
      unless files_to_rename.empty?
        puts "files to rename: "
        puts files_to_rename
      end

      files_to_modify = difference(unchanged_names, unchanged_etags)
      unless files_to_modify.empty?
        puts "files to modify: "
        puts files_to_modify
      end
    end
end

class LocalDirectory

  def initialize(path)
    @dir_path = path
  end

  def load_state
    dir_state = {:names => {}, :etags => {}}
    prev_dir = Dir.pwd
    Dir.chdir(@dir_path)
    files = Dir.glob('**/*')

    files.each do |file|
      if File.file?(file) && File.readable?(file)
        file_path = Dir.pwd + '/' + file
        etag = %x[md5 #{file_path}].split('=')[1].strip
        modified = File.stat(file).mtime
        file_info = {:name => file, :etag => etag, :modified => modified}
        dir_state[:names][file_info[:name]] = file_info
        dir_state[:etags][file_info[:etag]] = file_info
      end
    end

    Dir.chdir(prev_dir)

    dir_state
  end
end

def load_config
  config = YAML.load_file('config.yaml')
  @access_key_id = config['s3_info']['access_key_id']
  @secret_access_key = config['s3_info']['secret_access_key']
  @bucket = config['s3_info']['bucket']

  # set sync dir based on config.
  @sync_dir = config['s3_info']['sync_dir']
  Dir.chdir(Dir.pwd + '/' + @sync_dir)

  AWS.config(
    :access_key_id =>  @access_key_id, 
    :secret_access_key => @secret_access_key
  )
  @s3 = AWS::S3.new
  begin
    @s3_bucket = @s3.buckets[@bucket]
  rescue
    puts "Error loading bucket #{@bucket}."
  end
end


def load_s3_files
  @s3_files ||= {}
  begin
    @s3_bucket.objects.each do |obj|
      head = obj.head
      @s3_files[head.etag] = {:name => obj.key,
        :modified => head.last_modified}
    end
  rescue
    puts "Error loading file information from bucket #{@bucket}."
  end
end

def upload_file(file_name)
  begin
    @s3_bucket.objects[file_name].write(:file => file_name)
    puts "Uploading file #{file_name} to bucket #{@bucket}."
  rescue
    puts "Error uploading #{file_name} to bucket #{@bucket}."
  end
end

def get_file(file_name)
  begin
    File.open(file_name, 'w') do |file|
      begin
        puts "Downloading #{file_name} from bucket #{@bucket}"
        @s3_bucket.objects[file_name].read do |chunk|
          file.write(chunk)
        end
      rescue
        puts "Error downloading #{file_name} from s3"
      end
    end
  rescue
    puts "Error opening #{file_name}"
  end
end

def rename_file(old_name, new_name)
  begin
    @s3_bucket.objects[old_name].move_to(new_name)
    puts "Renaming #{old_name} to #{new_name}"
  rescue
    puts "Error renaming #{old_name} to #{new_name}"
  end
end

def delete_file(file_name)
  begin
    @s3_bucket.objects[file_name].delete
    puts "Deleting file #{file_name} from bucket #{@bucket}"
  rescue
    puts "Error deleting #{file_name} from bucket #{@bucket}"
  end
end


load_config
local_directory = LocalDirectory.new(Dir.pwd)
watcher = Watcher.new(local_directory, 1)
watcher.run
