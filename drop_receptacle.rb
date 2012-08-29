#!/usr/bin/env ruby

require 'rubygems'
require 'yaml'
require 'aws-sdk'
require 'observer'

class DirectoryWatcher
  include Observable

  def initialize(path)
    @base_path = path
  end

  def compare_states(prev, curr)

  end

  def run
    prev_dir_state = {}

    loop do
      curr_dir_state = LocalDirectory.load_state(@base_path)
      compare_states(prev_dir_state, curr_dir_state)

      sleep 10
    end
  end
end

class LocalDirectory

  def LocalDirectory.load_state(dir_path)
    dir_state = {:names => {}, :hashes => {}}
    prev_dir = Dir.pwd
    Dir.chdir(dir_path)
    files = Dir.glob('**/*')

    files.each do |file|
      if File.file?(file) && File.readable?(file)
        file_path = Dir.pwd + '/' + file
        hash = %x[md5 #{file_path}].split('=')[1].strip
        modified = File.stat(file).mtime
        file_info = {:name => file, :hash => hash, :modified => modified}
        dir_state[:names][file_info[:name]] = file_info
        dir_state[:hashes][file_info[:hash]] = file_info
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
