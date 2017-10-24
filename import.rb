require 'pg'

def read_file(filename)
  File.open(filename, 'r') do |csvfile|
    headers = csvfile.gets.split('|')
    
    while line = csvfile.gets
      params = {}
      fields = line.split('|')
      fields.each_with_index do |field, i|
        params[headers[i]] = field
      end

      yield(params)

    end
  end
end

def extract_date(s, format)
  return if s.nil? || s.length < format.length
  y = s[format.index('yyyy'), 4]
  m = s[format.index('mm'), 2]
  d = s[format.index('dd'), 2]
  return "#{y}-#{m}-#{d}"
end

def query(conn, sql, params)
  puts sql
  puts params
  conn.exec_params(sql, params)
end

def timestamps
  {created_at: 'now', updated_at: 'now'}
end

def prepare_params(params)
  params.transform_values!{ |v| v.gsub("'", "\\'") if v.respond_to? :gsub }
  params.merge timestamps
end

def insert_or_update(conn, table, params)
  params = prepare_params params
  sql = "SELECT id FROM #{table} WHERE id=$1;"
  existing = query(conn, sql, [params[:id]])

  fields = params.keys
  if existing.ntuples > 0
    sql = "UPDATE #{table} SET "
    sql += fields.collect{ |field| "#{field} = '#{params[field]}'"}.join(', ')
    sql += " WHERE id=#{params[:id]};"
  else
    sql = "INSERT INTO #{table} ("
    sql += fields.join(', ')
    sql += ") VALUES("
    sql += fields.collect{ |field| "'#{params[field]}'"}.join(', ')
    sql += ");"
  end
  puts sql
  conn.exec sql
end

def person_names(params)
  name = (params['Name']=='') ? params['ShortName'] : params['Name']
  split = name.index(' ')
  if split.nil?
    last = name
    first = ''
  else
    last = name[0, split]
    first = name[split+1 .. -1].lstrip
  end
  return first, last
end

def add_person(conn, params)
  first, last = person_names params
  gender = (params['Gender']=='F') ? '1' : '0'
  birth = extract_date params['BirthDate'], 'dd/mm/yyyy'
  save_params = {id: params['EmployeeID'], first_name: first, last_name: last, gender: gender}
  save_params[:birth_date] = birth unless birth.nil?
  insert_or_update conn, 'people', save_params
end

# ============ START HERE ===================================

conn = PG.connect(dbname: 'cmbpayroll_dev',
                  host: 'localhost',
                  user: 'cmbpayroll', 
                  password: 'cmbpayroll')

read_file('employees.csv') do |params|
  add_person conn, params
end


# ========== CSVImporter for model ==========================

class CSVImporter
  
    def self.import(filename, model, import_fields={})
      failed_records = []
      File.open(filename, 'r') do |csvfile|
        headers = csvfile.gets.split('|')
        headers.each_index { |i| headers[i] = dequote(headers[i]) }
  
        while line = csvfile.gets
          fields = {}
          farray = line.split('|')
          headers.each_index { |i| fields[headers[i]] = dequote(farray[i]) }
  
          params = {}
          import_fields.each_pair{ |model_sym, old_name| params[model_sym] = fields[old_name] }
          
          import_record = model.find_by(id: params[:id])
          import_record ||= model.new
  
          params = yield(params) if block_given?
  
          begin
              import_record.update params
          rescue => error
              failed_records << "Failed to import:\n  #{line}\n  Because #{error.message}\n"
          end
        end
      end
      failed_records.each{|line| p line}
    end
  
    def self.dequote(s)
      return if s.nil?
      s = s.chomp.chomp('"')
      s.slice!(0) if s[0] == '"'
      s
    end
  

  end