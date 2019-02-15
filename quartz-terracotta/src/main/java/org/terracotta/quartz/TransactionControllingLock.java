/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.terracotta.quartz;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import org.terracotta.toolkit.ToolkitFeatureTypeInternal;
import org.terracotta.toolkit.atomic.ToolkitTransaction;
import org.terracotta.toolkit.atomic.ToolkitTransactionController;
import org.terracotta.toolkit.atomic.ToolkitTransactionType;
import org.terracotta.toolkit.concurrent.locks.ToolkitLock;
import org.terracotta.toolkit.concurrent.locks.ToolkitLockType;
import org.terracotta.toolkit.internal.ToolkitInternal;
import org.terracotta.toolkit.rejoin.RejoinException;

/**
 *
 * @author cdennis
 */
class TransactionControllingLock implements ToolkitLock {

  private final ThreadLocal<HoldState> threadState = new ThreadLocal<HoldState>() {
    @Override
    protected HoldState initialValue() {
      return new HoldState();
    }
  };
  private final ToolkitTransactionController txnController;
  private final ToolkitTransactionType txnType;
  private final ToolkitLock delegate;
  
  public TransactionControllingLock(ToolkitInternal toolkit, ToolkitLock lock, ToolkitTransactionType txnType) {
    this.txnController = toolkit.getFeature(ToolkitFeatureTypeInternal.TRANSACTION);
    this.txnType = txnType;
    this.delegate = lock;
  }

  public Condition newCondition() throws UnsupportedOperationException {
    throw new UnsupportedOperationException();
  }

  public Condition getCondition() {
    throw new UnsupportedOperationException();
  }

  public ToolkitLockType getLockType() {
    return delegate.getLockType();
  }

  public boolean isHeldByCurrentThread() {
    return delegate.isHeldByCurrentThread();
  }

  public void lock() {
    delegate.lock();
    try {
      threadState.get().lock();
    } catch (RejoinException e) {
      delegate.unlock();
    }
  }

  public void lockInterruptibly() throws InterruptedException {
    delegate.lockInterruptibly();
    try {
      threadState.get().lock();
    } catch (RejoinException e) {
      delegate.unlock();
    }
  }

  public boolean tryLock() {
    if (delegate.tryLock()) {
      try {
        threadState.get().lock();
      } catch (RejoinException e) {
        delegate.unlock();
      }
      return true;
    } else {
      return false;
    }
  }

  public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
    if (delegate.tryLock(time, unit)) {
      try {
        threadState.get().lock();
      } catch (RejoinException e) {
        delegate.unlock();
      }
      return true;
    } else {
      return false;
    }
  }

  public void unlock() {
    try {
      threadState.get().unlock();
    } finally {
      delegate.unlock();
    }
  }

  public String getName() {
    return delegate.getName();
  }

  class HoldState {
    
    private ToolkitTransaction txnHandle;
    private int holdCount = 0;
    
    void lock() {
      if (holdCount++ == 0) {
        if (txnHandle == null) {
          txnHandle = txnController.beginTransaction(txnType);
        } else {
          throw new AssertionError();
        }
      }
    }

    void unlock() {
      if (--holdCount <= 0) {
        try {
          txnHandle.commit();
        } catch (RejoinException e) {
          throw new RejoinException("Exception caught during commit, transaction may or may not have committed.", e);
        } finally {
          threadState.remove();
        }
      }
    }
  }
}
