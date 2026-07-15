-- ============================================================
-- DuelIQ: server-authoritative wallet setup
-- Run this ONCE in Supabase: SQL Editor -> New query -> paste -> Run
-- ============================================================

-- ── 1. Atomic wallet functions (only the game server may call these) ──

create or replace function wallet_debit(p_user uuid, p_amount numeric)
returns numeric language plpgsql security definer as $$
declare new_balance numeric;
begin
  if p_amount <= 0 then return -1; end if;
  update profiles set wallet = round(wallet - p_amount, 2)
  where id = p_user and wallet >= p_amount
  returning wallet into new_balance;
  if new_balance is null then return -1; end if;  -- insufficient funds
  return new_balance;
end $$;

create or replace function wallet_credit(p_user uuid, p_amount numeric)
returns numeric language plpgsql security definer as $$
declare new_balance numeric;
begin
  if p_amount <= 0 then return -1; end if;
  update profiles set wallet = round(wallet + p_amount, 2)
  where id = p_user
  returning wallet into new_balance;
  return coalesce(new_balance, -1);
end $$;

-- Test credits: +$20, but only while balance is under $100
create or replace function test_credits(p_user uuid)
returns numeric language plpgsql security definer as $$
declare new_balance numeric;
begin
  update profiles set wallet = round(wallet + 20, 2)
  where id = p_user and wallet < 100
  returning wallet into new_balance;
  if new_balance is null then return -1; end if;  -- cap reached
  return new_balance;
end $$;

-- Clients must NOT be able to call these directly
revoke execute on function wallet_debit(uuid, numeric) from public, anon, authenticated;
revoke execute on function wallet_credit(uuid, numeric) from public, anon, authenticated;
revoke execute on function test_credits(uuid) from public, anon, authenticated;

-- ── 2. Lock down the profiles table ──

alter table profiles alter column wallet set default 0;
alter table profiles enable row level security;

drop policy if exists profiles_read on profiles;
drop policy if exists profiles_insert_own on profiles;
drop policy if exists profiles_update_own on profiles;

-- Anyone signed in can read profiles (needed for leaderboard names)
create policy profiles_read on profiles for select using (true);
-- You may create only your own profile row
create policy profiles_insert_own on profiles for insert with check (auth.uid() = id);
-- You may update only your own row (columns restricted below)
create policy profiles_update_own on profiles for update using (auth.uid() = id);

-- Column-level lock: clients may set username, NEVER wallet.
revoke insert, update on profiles from anon, authenticated;
grant select on profiles to anon, authenticated;
grant insert (id, username) on profiles to authenticated;
grant update (username) on profiles to authenticated;

-- ── 3. Lock down the matches table (server writes, clients read) ──

alter table matches enable row level security;
drop policy if exists matches_read on matches;
create policy matches_read on matches for select using (true);
-- No insert/update policy for clients: only the server (service_role) can write.
revoke insert, update, delete on matches from anon, authenticated;
grant select on matches to anon, authenticated;
