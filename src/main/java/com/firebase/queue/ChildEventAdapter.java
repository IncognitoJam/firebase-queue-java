package com.firebase.queue;


import com.google.firebase.database.ChildEventListener;
import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseError;

class ChildEventAdapter implements ChildEventListener {
    @Override
    public void onChildAdded(DataSnapshot dataSnapshot, String previousChildKey) {
    }

    @Override
    public void onChildChanged(DataSnapshot dataSnapshot, String previousChildKey) {
    }

    @Override
    public void onChildRemoved(DataSnapshot dataSnapshot) {
    }

    @Override
    public void onChildMoved(DataSnapshot dataSnapshot, String previousChildKey) {
    }

    @Override
    public void onCancelled(DatabaseError databaseError) {
    }
}
