require 'pg'
require 'date'

ERRORS = []
IMBALANCES = []

# FILE IO ======================================================

def read_file(filename)
  filename = "csv/#{filename}"
  File.open(filename, 'r') do |csvfile|
    headers = csvfile.gets.chomp.split('|', -1)
    
    linenumber = 1
    while line = csvfile.gets
      line.chomp!
      params = {}
      fields = line.split('|', -1)
      while fields.count < headers.count
        # puts "Headers: #{headers.count}. Fields: #{fields.count}"
        # puts headers
        # puts fields
        line2 = csvfile.gets.chomp
        fields2 = line2.split('|', -1)
        fields[-1] = (fields[-1] or '') + ' ' + (fields2[0] or '')
        fields += fields2.drop(1)
      end
      unless fields.count == headers.count
        raise "CSV Parse error - Unable to match fields to headers"
      end
      fields.each_with_index do |field, i|
        params[headers[i]] = field
      end
      params['linenumber'] = linenumber
      linenumber += 1

      # puts "PARAMS FROM FILE: #{params}"
      yield(params)

    end
  end
end

# PARSE Helpers =================================================

def dequote(s)
  return if s.nil?
  s = s.chomp.chomp('"')
  s.slice!(0) if s[0] == '"'
  s
end

def invalid_date?(y, m, d)
  return true unless (1900 .. Date.today.year) === y.to_i 
  return true unless (1 .. 12) === m.to_i
  return true unless (1 .. 31) === d.to_i  # yeah, it's not 100% reliable
  false
end

def extract_date(s, format='dd/mm/yyyy')
  return if s.nil? || s.length < format.length
  y = s[format.index('yyyy'), 4]
  m = s[format.index('mm'), 2]
  d = s[format.index('dd'), 2]
  if (1 .. 200) === y.to_i
    y = 1900 + y.to_i
  end
  invalid_date?(y, m, d) ? nil : "#{y}-#{m}-#{d}"
end

def timestamps(existing)
  existing ? {updated_at: 'now'} : 
             {created_at: 'now', updated_at: 'now'}
end

def prepare_params(params)
  params.transform_values! do |v| 
    v.respond_to?(:gsub) ? v.gsub("'", "''") : v  
  end
  # puts "PARAMS: #{params}"
  params
end

# DB Helpers =========================================================

def query(conn, sql, params=nil)
  puts "#{sql.gsub("\n", ' ')} [#{params}]"
  params.nil? ? 
          conn.exec(sql) :
          conn.exec_params(sql, params)
end

def find(conn, table, id)
  result = query conn, "SELECT * FROM #{table} WHERE id=#{id};"
  (result.ntuples > 0) ? result[0] : nil
end

def find_payslip(conn, employee_id, period_m, period_y)
  result = query conn, "SELECT * FROM payslips 
                        WHERE employee_id=#{employee_id} 
                        AND period_month=#{period_m} 
                        AND period_year=#{period_y};"
  (result.ntuples > 0) ? result[0] : nil
end

def payslip_exists?(conn, employee_id, period_m, period_y)
  not find_payslip(conn, employee_id, period_y, period_m).nil?
end

def find_last(conn, table)
  result = query conn, "SELECT * FROM #{table} ORDER BY id DESC LIMIT 1;"
  (result.ntuples > 0) ? result[0] : nil
end

def exists?(conn, table, id)
  result = query conn, "SELECT id FROM #{table} WHERE id=#{id};"
  return result.ntuples > 0
end

def update_sequence(conn, table)
  sequence_res = conn.exec("SELECT pg_get_serial_sequence('#{table}', 'id');")
  sequence = sequence_res[0]['pg_get_serial_sequence']
  seq_val = conn.exec("SELECT last_value FROM #{sequence};")[0]['last_value'].to_i
  biggest_id = conn.exec("SELECT MAX(id) FROM #{table};")[0]['max'].to_i
  
  unless seq_val > biggest_id
    restart = biggest_id.to_i + 1
    query(conn, "ALTER SEQUENCE #{sequence} RESTART WITH #{restart};")
  end
end

def insert(conn, table, params, include_timestamps=true)
  params = prepare_params(params)
  params.merge!(timestamps(false)) if include_timestamps
  fields = params.keys
  sql = "INSERT INTO #{table} ("
  sql += fields.join(', ')
  sql += ") VALUES("
  sql += fields.collect{ |field| "'#{params[field]}'"}.join(', ')
  sql += ");"
  puts query(conn, sql).cmd_status
  update_sequence(conn, table) if params[:id]
end

def update(conn, table, params)
  params = prepare_params(params).merge(timestamps(true))
  fields = params.keys
  sql = "UPDATE #{table} SET "
  sql += fields.collect{ |field| "#{field} = '#{params[field]}'"}.join(', ')
  sql += " WHERE id=#{params[:id]};"
  puts query(conn, sql).cmd_status
end

def insert_or_update(conn, table, params)
  if exists?(conn, table, params[:id])
    update conn, table, params
  else
    insert conn, table, params
  end
end

def smallest_free_id(conn, table)
  id = 1
  id += 1 while exists? conn, table, id
  id
end

# Employee Import ====================================================

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

def add_employee_person(conn, params)
  first, last = person_names params
  gender = (params['Gender']=='F') ? '1' : '0'
  birth = extract_date params['BirthDate'], 'dd/mm/yyyy'
  save_params = {id: params['EmployeeID'], first_name: first, last_name: last, gender: gender}
  save_params[:birth_date] = birth unless birth.nil?
  insert_or_update conn, 'people', save_params
end

def convert_category(c)
  c.to_i - 1
end

def convert_echelon(e)
  %w[1 2 3 4 5 6 7 8 9 10 11 12 13 A B C D E F G].index(e) 
end

def convert_wage_scale(w)
  %w[A B C D E].index(w)
end

def convert_wage_period(w)
  %w[H M].index(w)
end

def convert_employment_status(s)
  %w[F P T L Y I].index(s)
end

def convert_marital_status(s)
  %w[C M W].index(s)
end

def employee_params(params)
  # title, cnps, dipe, contract_start/end, category, echelon,
  # wage_scale, wage_period, last_raise_date, taxable_percentage,
  # transporation, employment_status, marital_status, hours_day,
  # days_week, wage, supervisor_id, department_id, amical, uniondues
  # first_day
  p = {}
  p[:id] = params['EmployeeID']
  p[:person_id] = params['EmployeeID']
  p[:title] = params['JobTitle']
  p[:cnps] = params['CNPSno']
  p[:dipe] = params['DIPESLine']
  p[:contract_start] = extract_date params['BeginContract'], 'dd/mm/yyyy'
  p[:first_day] = extract_date params['BeginContract'], 'dd/mm/yyyy'
  p[:contract_end] = extract_date params['EndContract'], 'dd/mm/yyyy'
  p[:category] = convert_category params['Category']
  p[:echelon] = convert_echelon params['Echelon']
  p[:wage_scale] = convert_wage_scale params['WageScale']
  p[:wage_period] = convert_wage_period params['CFAper']
  p[:last_raise_date] = extract_date params['LastVacation'], 'dd/mm/yyyy'
  p[:taxable_percentage] = params['TaxablePercent']
  p[:transportation] = params['Transportation']
  p[:employment_status] = convert_employment_status params['Status']
  p[:marital_status] = convert_marital_status params['MaritalStatus']
  p[:hours_day] = params['HoursPerDay'].to_i
  p[:days_week] = params['DaysPerWeek']
  p[:wage] = params['Wage']
  p[:supervisor_id] = params['SupervisorId']
  p[:department_id] = params['DepartmentId']
  p[:amical] = params['AMICAL']
  p[:uniondues] = params['Union']
  p.keys.each do |key|
    if p[key].nil? or p[key] == ''
      p.delete key
    end
  end
  # puts p
  p
end


def add_employee(conn, params)
  emp_params = employee_params params
  unless exists?(conn, 'supervisors', emp_params[:supervisor_id])
    emp_params.delete(:supervisor_id)
  end
  unless exists?(conn, 'departments', emp_params[:department_id])
    emp_params.delete(:department_id)
  end

  insert_or_update conn, 'employees', emp_params
end
  

# Child Import ====================================================

def child_names(name)
  split = name.index(' ')
  if split.nil?
    first = name
    last = ''
  else
    last = name[0, split]
    first = name[split+1 .. -1].lstrip
  end
  return first, last
end

def insert_child(conn, person_params, child_params)
  conn.transaction do |conn|
    insert conn, 'people', person_params
    person = find_last conn, 'people'
    child_params[:person_id] = person['id']
    insert conn, 'children', child_params
  end
end

def update_child(conn, child, person_params, child_params)
  update conn, 'children', child_params
  person_params[:id] = child['person_id']
  update conn, 'people', person_params
end

def add_child(conn, params)
  # Don't add children if the parent doesn't exist
  return unless exists? conn, 'people', params['EmployeeId']

  first, last = child_names params['Name']
  birth = extract_date params['BirthDate'], 'dd/mm/yyyy'
  person_params = {first_name: first, 
                   last_name: last,}
  person_params[:birth_date] = birth unless birth.nil?                   
  child_params = {id: params['linenumber'], 
                  parent_id: params['EmployeeId'], 
                  is_student: params['Student']}
  #puts "CHILD PARAMS: #{child_params}"

  child = find conn, 'children', child_params[:id]
  if child.nil?
    insert_child conn, person_params, child_params
  else
    update_child conn, child, person_params, child_params
  end
end

# Department Import ===============================================

def add_deparment(conn, params)
  dpt_params = {id: params['DepartmentId'],
                name: params['Name'],
                description: params['Description'],
                account: params['Account']}
  insert_or_update conn, 'departments', dpt_params
end

# Supervisor Import ==============================================

def find_matching_person(conn, full_name)
  full_name.split(/[\. ]/).each do |name|
    if name.length > 1
      sql = "SELECT id, first_name, last_name FROM people 
             WHERE first_name ILIKE '%#{name}%' OR
                   last_name  ILIKE '%#{name}%';"
      result = query conn, sql
      result.each do |row|
        puts "Is #{full_name} the same as #{row['first_name']} #{row['last_name']}?"
        if gets[0].downcase == 'y'
          return row['id']
        end
      end
    end
  end
  return nil
end

def supervisor_names(name)
  split = name.rindex(/[\. ]/)
  if split.nil?
    return '', name
  end
  first = name.slice(0, split)
  last = name.slice (split+1 .. -1)
  return first, last
end

def add_supervisor(conn, params)
  # unless exists? conn, 'supervisors', params['SupervisorId']
  last_sup = find_last(conn, 'supervisors')
  if last_sup.nil? or params['SupervisorId'].to_i > last_sup['id'].to_i
    person_id = find_matching_person(conn, params['Name'])
    if person_id.nil?
      first, last = supervisor_names params['Name']
      conn.transaction do |conn|
        insert conn, 'people', {first_name: first, last_name: last}
        person_id = find_last(conn, 'people')['id']
        insert conn, 'supervisors', {id: params['SupervisorId'], person_id: person_id}
      end
    else
      insert conn, 'supervisors', {id: params['SupervisorId'], person_id: person_id}
    end
  end
end

def merge_duplicate_supervisors(conn)
  sql = "SELECT person_id FROM supervisors GROUP BY person_id HAVING count(id)>1;"
  result = query conn, sql
  result.each do |row|
    result_dups = query conn, "SELECT * FROM supervisors 
                               WHERE person_id=#{row['person_id']};"
    supervisor_id = result_dups[0]['id']                               
    (1 .. (result_dups.ntuples - 1)).each do |index|
      query conn, "UPDATE employees 
                   SET supervisor_id=#{supervisor_id} 
                   WHERE supervisor_id=#{result_dups[index]['id']};"

      query conn, "DELETE FROM supervisors WHERE id=#{result_dups[index]['id']};"
    end
  end
end                   

# Payslip Import ============================================

def parse_period(period)
  m = period[0, 2].to_i
  y = period[3, 2].to_i
  y = (y > 20) ? y + 1900 : y + 2000
  return m, y
end

def previous_period(month, year)
  if month > 1
    return (month - 1), year
  end
  return 12, (year - 1)
end

def date_from_period(period)
  month, year = parse_period period
  month = 12 if month > 12
  mtext = (month > 9) ? month.to_s : "0#{month}"
  "#{year}-#{mtext}-01"
end

def add_payslip_earnings_deductions(conn, params)
  query conn, "DELETE FROM earnings WHERE payslip_id=#{params['PaySlipID']};"
  query conn, "DELETE FROM deductions WHERE payslip_id=#{params['PaySlipID']};"

  %w[BonusPrimeExcep BonusOther MiscPay1 Transport].each do |earning|
    if params[earning].to_i > 0
      earning_params = { payslip_id: params['PaySlipID'],
                         amount: params[earning] }
      earning_params[:description] = 
        case earning
        when 'BonusOther'
          params['BonusOtherDesc']
        when 'MiscPay1'
          params['MiscDesc1']
        else
          earning
        end
      earning_params[:is_bonus] = 
        case earning
        when 'BonusOther', 'BonusPrimeExcep'
          'true'
        else
          'false'
        end
      insert conn, 'earnings', earning_params
    end
  end

  %w[Union Photocopies Telephone Rent Water Electricity AMICAL Other].each do |deduction|
    if params[deduction].to_i > 0
      deduction_params = { payslip_id: params['PaySlipID'],
                           date: date_from_period(params['Period']),
                           amount: params[deduction],
                           note: deduction }
      insert conn, 'deductions', deduction_params
    end
  end
end

def add_payslip(conn, params)
  month, year = parse_period params['Period']
  ps_params = { id: params['PaySlipID'],
                employee_id: params['EmployeeId'],
                period_year: year,
                period_month: month,
                category: convert_category(params['Category']),
                echelon: convert_echelon(params['Echelon']),
                wagescale: convert_wage_scale(params['WageScale']),
                basewage: params['BaseWage'],
                days: params['Days'],
                hours: params['Hours'],
                overtime_hours: params['OvertimeHours'],
                overtime2_hours: params['OvertimeHours2'],
                overtime3_hours: params['OvertimeHours3'],
                overtime_rate: params['OvertimeRate'],
                overtime2_rate: params['OvertimeRate2'],
                overtime3_rate: params['OvertimeRate3'],
                caissebase: params['CaisseBase'],
                cnpswage: params['CNPSWage'],
                taxable: params['Taxable'],
                proportional: params['ProportionalTax'],
                communal: params['CommunalTax'],
                cac: params['CAC'],
                cac2: params['CAC2'],
                cnps: params['CNPS'],
                ccf: params['CCF'],
                crtv: params['CRTV']
              }
  ps_params.delete_if{ |key, value| value.nil? or value == ''}         

  insert_or_update conn, 'payslips', ps_params
  
  add_payslip_earnings_deductions(conn, params)
end

# Payslip Vacation Import ==================================

def payslip_vacation_balance(conn, params)
  # Skip the malformatted ones
  return unless (params['Period'].include? '/')

  month, year = parse_period params['Period']
  days_start_balance = params['PrevVacDays'].to_f + params['SupVacDaysInPeriod'].to_f
  days_earned = params['VacationDays'].to_f + params['SupVacDays'].to_f
  days_balance = days_start_balance + days_earned

  vpay_start_balance = params['PrevVacPay'].to_f.to_i + params['SupVacPayInPeriod'].to_i
  vpay_earned = params['VacationPay'].to_f.to_i + params['SupVacPayInPeriod'].to_i
  vpay_balance = vpay_start_balance + vpay_earned

  ps_params = {id: params['PaySlipID'],
               employee_id: params['EmployeeId'],
               period_year: year,
               period_month: month,
               vacation_earned: days_earned,
               vacation_balance: days_balance,
               vacation_pay_earned: vpay_earned,
               vacation_pay_balance: vpay_balance}
  
  insert_or_update conn, 'payslips', ps_params
end

def add_vacation(conn, params)
  month, year = parse_period params['Period']
  start_date = extract_date params['VacationStart'], 'dd/mm/yyyy'
  end_date = extract_date params['VacationEnd'], 'dd/mm/yyyy'
  days_used = params['VacationDays'].to_f.to_i
  pay_used = params['VacationPay'].to_f.to_i

  unless start_date.nil? or end_date.nil?
    v_params = { id: params['PaySlipID'],
                employee_id: params['EmployeeId'],
                start_date: start_date,
                end_date: end_date}

    insert_or_update conn, 'vacations', v_params
  end
  
  payslip = find_payslip conn, params['EmployeeId'], month, year
  unless payslip.nil?
    days_balance = payslip['vacation_balance'].to_f + days_used
    pay_balance = payslip['vacation_pay_balance'].to_i + pay_used

    ps_params = { id: payslip['id'],
                  vacation_used: (days_used * -1),
                  vacation_balance: days_balance,
                  vacation_pay_used: (pay_used * -1),
                  vacation_pay_balance: pay_balance }

    update conn, 'payslips', ps_params
  end
end

def round_to_nearest_half(num)
  (num * 2).round.to_f / 2
end

def normalize_vacay_balances(conn)
  result = query conn, "SELECT year, month FROM last_posted_periods;"
  posted = result[0]
  sql = "SELECT id, vacation_balance FROM payslips 
         WHERE period_year=#{posted['year']} 
         AND period_month=#{posted['month']};"
  payslips = query conn, sql
  payslips.each do |payslip|
    balance = payslip['vacation_balance'].to_f
    new_balance = round_to_nearest_half balance
    # puts "Change #{balance} to #{new_balance}"
    update conn, 'payslips', {id: payslip['id'], vacation_balance: new_balance}
  end
end

# Payslip Loan Import ========================================

def add_loan_from_payslip(conn, params)
  loan_params = { id: params['PaySlipID'],
                  employee_id: params['EmployeeId'],
                  amount: params['NewLoan'],
                  origination: date_from_period(params['Period'])}
  insert_or_update conn, 'loans', loan_params
end

def add_loan_payments(conn, employee_id, payment_amount, date)
  payment_params = { date: date }

  my_loans = query conn, "SELECT * FROM loans 
                          WHERE employee_id=#{employee_id} ORDER BY id;"
  i = 0
  while(i < my_loans.ntuples)
    loan = my_loans[i]
    paid = query(conn, "SELECT SUM(amount) FROM loan_payments 
                        WHERE loan_id=#{loan['id']};")[0]['sum'].to_i
    if loan['amount'].to_i > paid
      remaining = loan['amount'].to_i - paid
      if remaining >= payment_amount
        payment_params[:amount] = payment_amount
        payment_params[:loan_id] = loan['id']
        insert conn, 'loan_payments', payment_params
        return 0
      else
        payment_amount = payment_amount - remaining
        payment_params[:amount] = remaining
        payment_params[:loan_id] = loan['id']
        insert conn, 'loan_payments', payment_params
      end
    end
    i += 1
  end
  return payment_amount
end

def add_loan_payment_from_payslip(conn, params)
  date = date_from_period(params['Period'])
  remainder = add_loan_payments conn, 
                                params['EmployeeId'], 
                                params['LoanPayment'].to_i,
                                date
  if remainder != 0
    ERRORS << "Loan Balance error for payslip ##{params['PaySlipID']}."
  end
end

def payslip_loans(conn, params)
  # Skip the malformatted ones
  return unless (params['Period'].include? '/')

  if params['NewLoan'].to_i > 0
    add_loan_from_payslip conn, params
  end
end

def payslip_loan_payments(conn, params)
  # Skip the malformatted ones
  return unless (params['Period'].include? '/')
  
  if params['LoanPayment'].to_i > 0
    add_loan_payment_from_payslip conn, params
  end
end

def add_correction_loan(conn, employee_id, amount)
  id = smallest_free_id conn, 'loans'
  loan_params = { id: id,
                  employee_id: employee_id,
                  amount: amount,
                  origination: Date.today.to_s, 
                  comment: "Auto-created to match existing balance." }
  insert conn, 'loans', loan_params
end

def add_correction_payments(conn, employee_id, amount)
  remainder = add_loan_payments(conn, employee_id, amount, Date.today.to_s)
  if(remainder != 0)
    raise "Had a remainder of #{amount} after adding correction payments !"
  end
end

def normalize_loan_balance(conn, params)
  sql = "SELECT sum(amount) FROM loans WHERE employee_id=#{params['EmployeeId']};"
  loan_total = query(conn, sql)[0]['sum'].to_i
  sql = "SELECT sum(loan_payments.amount) FROM loans 
          INNER JOIN loan_payments ON loans.id=loan_id 
          WHERE employee_id=#{params['EmployeeId']};"
  payment_total = query(conn, sql)[0]['sum'].to_i
  balance = loan_total - payment_total

  check_balance = params['PrevLoan'].to_i + 
                  params['NewLoan'].to_i - 
                  params['LoanPayment'].to_i
  check_balance = 0 if check_balance < 0

  if balance != check_balance
    IMBALANCES << "[#{params['EmployeeId']}] #{params['Name']} Calc: #{balance}. Payslip: #{check_balance}."                  
    if balance < check_balance
      add_correction_loan(conn, params['EmployeeId'], check_balance - balance)
    else
      add_correction_payments(conn, params['EmployeeId'], balance - check_balance)
    end
  end
end

# Transaction Import ========================================

def add_charge(conn, params)
  date = extract_date params['TransDate']
  amount = (params['Quantity'].to_f.to_i * -1)
  return if date.nil?
  charge_params = { id: params['TransID'],
                    date: date,
                    amount: amount,
                    employee_id: params['EmployeeId'],
                    note: "[#{params['CodeId']}] #{params['Comment']}" }

  insert_or_update conn, 'charges', charge_params                    
end

# DEP
def add_loan(conn, params)
  date = extract_date params['TransDate']
  return if date.nil?
  amount = params['Quantity'].to_f.to_i
  loan_params = { id: params['TransID'],
                  amount: amount,
                  comment: params['Comment'],
                  employee_id: params['EmployeeId'],
                  origination: date }

  insert_or_update conn, 'loans', loan_params                  
end

# DEP
def find_loan(conn, employee_id, payment_date)
  sql = "SELECT id FROM loans WHERE origination < '#{payment_date}' ORDER BY origination DESC LIMIT 1;"
  loans = query conn, sql
  return (loans.ntuples > 0) ? loans[0] : nil
end

# DEP
def add_loan_payment(conn, params)
  date = extract_date params['TransDate']
  return if date.nil?
  loan = find_loan conn, params['EmployeeId'], date
  return if loan.nil?
  amount = params['Quantity'].to_f.to_i

  payment_params = { id: params['TransID'],
                     loan_id: loan['id'],
                     amount: amount,
                     date: date }

  insert_or_update conn, 'loan_payments', payment_params                     
end

def add_transaction(conn, params)
  if exists?(conn, 'employees', params['EmployeeId'])
    case params['CodeId']
    when 'EL', 'MC', 'PC', 'RE', 'TC', 'WC', 'MP'
      add_charge conn, params
    when 'NL'
      # add_loan conn, params
    when 'LN'
      # add_loan_payment conn, params
    end
  end
end

# Bonuses =================================================

def add_bonus(conn, params)
  type = (params['BonusUnits'] == 'CFA') ? 1 : 0
  quantity = params['BonusQuantity']
  quantity = params['BonusQuantity'].to_f / 100 if type == 0
  bonus_params = { id: params['BonusID'],
                   name: params['BonusName'],
                   quantity: quantity,
                   bonus_type: type,
                   comment: params['Comment'] }

  insert_or_update conn, 'bonuses', bonus_params
end 

def add_employee_bonuses(conn, params)
  (1 .. 10).each do |i|
    field = "Bonus#{i}"
    if params[field] == '1'
      insert conn, 
             'bonuses_employees', 
             {employee_id: params['EmployeeID'], bonus_id: i}, 
             false
    end
  end

  if params['PrimeCaisse'].to_f != 0
    percent = (params['PrimeCaisse'].to_f * 100).to_i
    percent = percent.to_f / 100
    pc_bonus_res = query(conn, "SELECT id FROM bonuses 
                                WHERE name='PrimeCaisse' 
                                AND quantity=#{percent};")
    if pc_bonus_res.ntuples == 0
      b_params = { name: 'PrimeCaisse', 
                   quantity: percent,
                   bonus_type: 0 }
      insert conn, 'bonuses', b_params
      pc_bonus = find_last(conn, 'bonuses')
    else
      pc_bonus = pc_bonus_res[0]
    end
    bonus_id = pc_bonus['id']
    insert conn,
           'bonuses_employees',
           {employee_id: params['EmployeeID'], bonus_id: bonus_id },
           false
  end
end                                              

# Grouped Adds ===========================================

def normal_valid_payslip?(conn, params)
  params['Period'].include? '/' and # Skip the malformatted ones
    params['Type'] == 'P' and 
    exists?(conn, 'employees', params['EmployeeId'])
end

def add_people(conn)
  read_file('departments.csv') do |params|
    add_deparment conn, params
    puts ''
  end

  read_file('employees.csv') do |params|
    add_employee_person conn, params
    puts ''
  end

  read_file('supervisors.csv') do |params|
    add_supervisor conn, params
    puts ''
  end

  read_file('employees.csv') do |params|
    add_employee conn, params
    puts ''
  end

  read_file('children.csv') do |params|
    add_child conn, params
    puts ''
  end

  merge_duplicate_supervisors conn
end

def add_vacations(conn)
  read_file('payslip_history.csv') do |params|
    if params['Type'] == 'P' and exists?(conn, 'employees', params['EmployeeId'])
      payslip_vacation_balance conn, params
    end
  end

  read_file('payslips.csv') do |params|
    if params['Type'] == 'P' and exists?(conn, 'employees', params['EmployeeId'])
      payslip_vacation_balance conn, params
    end
  end

  read_file('payslips.csv') do |params|
    if params['Type'] == 'V' and exists?(conn, 'employees', params['EmployeeId'])
      add_vacation conn, params
    end
  end

  read_file('payslip_history.csv') do |params|
    if params['Type'] == 'V' and exists?(conn, 'employees', params['EmployeeId'])
      add_vacation conn, params
    end
  end

  normalize_vacay_balances conn
end

def add_loans(conn)
  read_file('payslip_history.csv') do |params|
    if params['Type'] == 'P' and exists?(conn, 'employees', params['EmployeeId'])
      payslip_loans conn, params
    end
  end
  
  read_file('payslips.csv') do |params|
    if params['Type'] == 'P' and exists?(conn, 'employees', params['EmployeeId'])
      payslip_loans conn, params
    end
  end
  
  query conn, "DELETE FROM loan_payments;"
  read_file('payslip_history.csv') do |params|
    if params['Type'] == 'P' and exists?(conn, 'employees', params['EmployeeId'])
      payslip_loan_payments conn, params
    end
  end
  
  read_file('payslips.csv') do |params|
    if params['Type'] == 'P' and exists?(conn, 'employees', params['EmployeeId'])
      payslip_loan_payments conn, params
    end
  end

  read_file('payslips.csv') do |params|
    if params['Period'].include? '/' # Skip the malformatted ones
      if params['Type'] == 'P' and exists?(conn, 'employees', params['EmployeeId'])
        last_payslip = query(conn, "SELECT * FROM payslips 
                                    WHERE employee_id=#{params['EmployeeId']} 
                                    ORDER BY period_year DESC, period_month DESC 
                                    LIMIT 1;")[0]
        if params['Period'] == "#{last_payslip['period_month']}/#{last_payslip['period_year'][2, 2]}"
          normalize_loan_balance conn, params
        else
          puts ''
          puts "Not last payslip: #{params['Period']} vs. #{last_payslip['period_month']}/#{last_payslip['period_year']}."
          puts ''
        end
      end
    end
  end
  puts "Imbalances:"
  IMBALANCES.each{ |s| puts s }
  puts ''
end

def add_transactions(conn)
  read_file('transaction_history.csv') do |params|
    add_transaction conn, params
  end

  read_file('transactions.csv') do |params|
    add_transaction conn, params
  end
end

def add_bonuses(conn)
  read_file('bonuses.csv') do |params|
    add_bonus conn, params
  end

  query conn, "DELETE FROM bonuses_employees;"
  read_file('employees.csv') do |params|
    add_employee_bonuses conn, params
  end
end

def add_payslips(conn)
  read_file('payslip_history.csv') do |params|
    if normal_valid_payslip? conn, params
      add_payslip conn, params
    end
  end

  read_file('payslips.csv') do |params|
    if normal_valid_payslip? conn, params
      add_payslip conn, params
    end
  end
end

# ============ START HERE ===================================
# ===========================================================

conn = PG.connect(dbname: 'cmbpayroll_dev',
                  host: 'localhost',
                  user: 'cmbpayroll', 
                  password: 'cmbpayroll')

# add_people conn
# add_vacations conn
# add_loans conn
# add_transactions conn
# add_bonuses conn
add_payslips conn

ERRORS.each do |error|
  puts error
end